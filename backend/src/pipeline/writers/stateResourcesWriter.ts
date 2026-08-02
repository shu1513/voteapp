import { Pool, type PoolClient } from "pg";
import { createClient } from "redis";

import {
  STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
} from "../../contracts/stateResourceEnrichmentContract.js";
import { parseCanonicalStateResourcePayload } from "../../contracts/stateResourcePayloadContract.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STAGING_STATE_RESOURCES_WRITER_GROUP,
  STAGING_VALIDATED_STREAM,
  STAGING_WRITTEN_STREAM,
} from "../../config/stateResourcePipeline.js";
import type { StateResourcePayload } from "../../types/stateResource.js";
import { createStageObserver } from "../utils/observability.js";
import { hasRunIdMismatch, normalizeRunId } from "../utils/runIdGuard.js";

type WriterOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
  reclaimMinIdleMs?: number;
};

type StagingRow = {
  ingest_key: string;
  item_type: string;
  run_id: string | null;
  schema_version: string | null;
  model: string | null;
  prompt_version: string | null;
  reason: string | null;
  payload: unknown;
  status: string;
};

type WriterOutcome = "written" | "failed" | "skipped" | "retry" | "recovered";

type WriterProcessResult = {
  outcome: WriterOutcome;
  reason: string | null;
  schemaVersion: string | null;
  promptVersion: string | null;
  provider: string | null;
  model: string | null;
};

type ParseResult =
  | { ok: true; payload: StateResourcePayload }
  | { ok: false; reason: string };

const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;

/**
 * Converts unknown errors into a bounded reason string suitable for DB persistence.
 */
function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

/**
 * Returns true when value is a non-empty string after trimming.
 */
function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function parseProviderModel(value: string | null): { provider: string | null; model: string | null } {
  if (typeof value !== "string") {
    return { provider: null, model: null };
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return { provider: null, model: null };
  }

  const separator = trimmed.indexOf(":");
  if (separator <= 0 || separator === trimmed.length - 1) {
    return { provider: null, model: trimmed };
  }

  return {
    provider: trimmed.slice(0, separator),
    model: trimmed.slice(separator + 1),
  };
}

/**
 * Parses a validated staging payload into the strict StateResourcePayload shape.
 */
function parseStateResourcePayload(payload: unknown): ParseResult {
  return parseCanonicalStateResourcePayload(payload);
}

/**
 * Ensures the Redis consumer group exists for the validated stream.
 */
async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, "0", {
      MKSTREAM: true,
    });
  } catch (error) {
    const message = toReason(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

/**
 * Loads one staging item by ingest key.
 */
async function getStagingRow(pool: Pool, ingestKey: string): Promise<StagingRow | null> {
  const result = await pool.query<StagingRow>(
    `
      SELECT ingest_key, item_type, run_id, schema_version, model, prompt_version, reason, payload, status
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_STATE_RESOURCES]
  );

  return result.rows[0] ?? null;
}

/**
 * Returns current staging status for one ingest key.
 */
async function getStagingStatus(pool: Pool, ingestKey: string): Promise<string | null> {
  const result = await pool.query<{ status: string }>(
    `
      SELECT status
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_STATE_RESOURCES]
  );

  return result.rows[0]?.status ?? null;
}

/**
 * Marks a staging row as failed, preserving the reason for investigation and retry logic.
 */
async function markFailed(
  pool: Pool,
  ingestKey: string,
  reason: string,
  expectedRunId: string | null
): Promise<void> {
  await pool.query(
    `
      UPDATE staging_items
      SET status = 'failed',
          reason = $2,
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $3
        AND status = 'validated'
        AND run_id IS NOT DISTINCT FROM $4
    `,
    [ingestKey, reason, STAGING_ITEM_TYPE_STATE_RESOURCES, expectedRunId]
  );
}

/**
 * Upserts one state_resources row and transitions the staging row from validated -> written.
 */
async function writeStateResourceAndMarkWritten(
  client: PoolClient,
  ingestKey: string,
  payload: StateResourcePayload,
  expectedRunId: string | null
): Promise<boolean> {
  await client.query("BEGIN");

  try {
    const statusUpdate = await client.query(
      `
        UPDATE staging_items
        SET status = 'written',
            written_at = now(),
            updated_at = now()
        WHERE ingest_key = $1
          AND item_type = $2
          AND status = 'validated'
          AND run_id IS NOT DISTINCT FROM $3
      `,
      [ingestKey, STAGING_ITEM_TYPE_STATE_RESOURCES, expectedRunId]
    );

    if (statusUpdate.rowCount !== 1) {
      await client.query("ROLLBACK");
      return false;
    }

    await client.query(
      `
        INSERT INTO state_resources (
          state_fips,
          state_abbreviation,
          state_name,
          polling_place_url,
          voter_registration_url,
          mail_voting_available,
          mail_ballot_request_url,
          mail_ballot_request_type,
          mail_ballot_request_deadline_rule,
          mail_ballot_return_deadline_rule,
          mail_ballot_return_deadline_type,
          early_voting_available,
          early_voting_start_date_rule,
          early_voting_end_date_rule,
          polling_hours,
          id_requirements,
          same_day_registration_available,
          online_registration_available,
          online_registration_deadline_rule,
          in_person_registration_deadline_rule,
          sources
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21::jsonb
        )
        ON CONFLICT (state_fips) DO UPDATE SET
          state_abbreviation = EXCLUDED.state_abbreviation,
          state_name = EXCLUDED.state_name,
          polling_place_url = EXCLUDED.polling_place_url,
          voter_registration_url = EXCLUDED.voter_registration_url,
          mail_voting_available = EXCLUDED.mail_voting_available,
          mail_ballot_request_url = EXCLUDED.mail_ballot_request_url,
          mail_ballot_request_type = EXCLUDED.mail_ballot_request_type,
          mail_ballot_request_deadline_rule = EXCLUDED.mail_ballot_request_deadline_rule,
          mail_ballot_return_deadline_rule = EXCLUDED.mail_ballot_return_deadline_rule,
          mail_ballot_return_deadline_type = EXCLUDED.mail_ballot_return_deadline_type,
          early_voting_available = EXCLUDED.early_voting_available,
          early_voting_start_date_rule = EXCLUDED.early_voting_start_date_rule,
          early_voting_end_date_rule = EXCLUDED.early_voting_end_date_rule,
          polling_hours = EXCLUDED.polling_hours,
          id_requirements = EXCLUDED.id_requirements,
          same_day_registration_available = EXCLUDED.same_day_registration_available,
          online_registration_available = EXCLUDED.online_registration_available,
          online_registration_deadline_rule = EXCLUDED.online_registration_deadline_rule,
          in_person_registration_deadline_rule = EXCLUDED.in_person_registration_deadline_rule,
          sources = EXCLUDED.sources
      `,
      [
        payload.state_fips,
        payload.state_abbreviation,
        payload.state_name,
        payload.polling_place_url,
        payload.voter_registration_url,
        payload.mail_voting_available,
        payload.mail_ballot_request_url,
        payload.mail_ballot_request_type,
        payload.mail_ballot_request_deadline_rule,
        payload.mail_ballot_return_deadline_rule,
        payload.mail_ballot_return_deadline_type,
        payload.early_voting_available,
        payload.early_voting_start_date_rule,
        payload.early_voting_end_date_rule,
        payload.polling_hours,
        payload.id_requirements,
        payload.same_day_registration_available,
        payload.online_registration_available,
        payload.online_registration_deadline_rule,
        payload.in_person_registration_deadline_rule,
        JSON.stringify(payload.sources),
      ]
    );

    await client.query("COMMIT");
    return true;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/**
 * Reclaims stale pending entries for this consumer group so crashes don't strand messages.
 */
async function reclaimPendingEntries(
  redis: ReturnType<typeof createClient>,
  consumerName: string,
  batchSize: number,
  reclaimMinIdleMs: number
): Promise<Array<{ id: string; message: Record<string, string> }>> {
  const reclaimed: Array<{ id: string; message: Record<string, string> }> = [];
  let cursor = "0-0";

  for (let i = 0; i < RECLAIM_MAX_BATCHES; i += 1) {
    const claim = await redis.xAutoClaim(
      STAGING_VALIDATED_STREAM,
      STAGING_STATE_RESOURCES_WRITER_GROUP,
      consumerName,
      reclaimMinIdleMs,
      cursor,
      { COUNT: batchSize }
    );

    cursor = claim.nextId;

    if (!claim.messages || claim.messages.length === 0) {
      break;
    }

    reclaimed.push(
      ...claim.messages
        .filter((entry): entry is NonNullable<typeof entry> => entry !== null)
        .map((entry) => ({ id: entry.id, message: entry.message as Record<string, string> }))
    );
  }

  return reclaimed;
}

/**
 * Processes one message from the validated stream.
 */
async function processMessage(
  pool: Pool,
  redis: ReturnType<typeof createClient>,
  messageId: string,
  message: Record<string, string>
): Promise<WriterProcessResult> {
  const ingestKey = message.ingest_key;
  const messageRunId = normalizeRunId(message.run_id);

  if (!ingestKey) {
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "skipped",
      reason: null,
      schemaVersion: null,
      promptVersion: null,
      provider: null,
      model: null,
    };
  }

  const row = await getStagingRow(pool, ingestKey);

  if (!row) {
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "skipped",
      reason: null,
      schemaVersion: null,
      promptVersion: null,
      provider: null,
      model: null,
    };
  }
  const providerModel = parseProviderModel(row.model);

  if (hasRunIdMismatch(message.run_id, row.run_id)) {
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "skipped",
      reason: row.reason,
      schemaVersion: row.schema_version,
      promptVersion: row.prompt_version,
      provider: providerModel.provider,
      model: providerModel.model,
    };
  }
  const expectedRunId = messageRunId ?? normalizeRunId(row.run_id);

  if (row.status === "written") {
    // Recovery path: if prior publish/ack failed post-commit, message may be redelivered.
    // Downstream consumers must dedupe by ingest_key for at-least-once delivery.
    try {
      await redis.xAdd(STAGING_WRITTEN_STREAM, "*", {
        ingest_key: ingestKey,
        item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
        run_id: row.run_id ?? "",
      });
      await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
      return {
        outcome: "recovered",
        reason: row.reason,
        schemaVersion: row.schema_version,
        promptVersion: row.prompt_version,
        provider: providerModel.provider,
        model: providerModel.model,
      };
    } catch {
      // Leave unacked so it can be reclaimed and retried.
      return {
        outcome: "retry",
        reason: "redis publish/ack failed while recovering written row",
        schemaVersion: row.schema_version,
        promptVersion: row.prompt_version,
        provider: providerModel.provider,
        model: providerModel.model,
      };
    }
  }

  if (row.status !== "validated") {
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "skipped",
      reason: row.reason,
      schemaVersion: row.schema_version,
      promptVersion: row.prompt_version,
      provider: providerModel.provider,
      model: providerModel.model,
    };
  }

  if (!isNonEmptyString(row.prompt_version)) {
    const reason = "prompt_version metadata is required";
    await markFailed(pool, ingestKey, reason, expectedRunId);
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "failed",
      reason,
      schemaVersion: row.schema_version,
      promptVersion: row.prompt_version,
      provider: providerModel.provider,
      model: providerModel.model,
    };
  }

  if (row.schema_version !== STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION) {
    const reason = `schema_version must be ${STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION} for writer input`;
    await markFailed(
      pool,
      ingestKey,
      reason,
      expectedRunId
    );
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "failed",
      reason,
      schemaVersion: row.schema_version,
      promptVersion: row.prompt_version,
      provider: providerModel.provider,
      model: providerModel.model,
    };
  }

  const parsed = parseStateResourcePayload(row.payload);
  if (!parsed.ok) {
    await markFailed(pool, ingestKey, parsed.reason, expectedRunId);
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "failed",
      reason: parsed.reason,
      schemaVersion: row.schema_version,
      promptVersion: row.prompt_version,
      provider: providerModel.provider,
      model: providerModel.model,
    };
  }

  const client = await pool.connect();
  try {
    const didTransitionToWritten = await writeStateResourceAndMarkWritten(
      client,
      ingestKey,
      parsed.payload,
      expectedRunId
    );

    if (!didTransitionToWritten) {
      await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
      return {
        outcome: "skipped",
        reason: null,
        schemaVersion: row.schema_version,
        promptVersion: row.prompt_version,
        provider: providerModel.provider,
        model: providerModel.model,
      };
    }
  } catch (error) {
    const reason = toReason(error);
    await markFailed(pool, ingestKey, reason, expectedRunId);
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "failed",
      reason,
      schemaVersion: row.schema_version,
      promptVersion: row.prompt_version,
      provider: providerModel.provider,
      model: providerModel.model,
    };
  } finally {
    client.release();
  }

  // Row is committed as written. If publish/ack fails, keep message pending for reclaim.
  try {
    await redis.xAdd(STAGING_WRITTEN_STREAM, "*", {
      ingest_key: ingestKey,
      item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
      run_id: row.run_id ?? "",
    });
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return {
      outcome: "written",
      reason: row.reason,
      schemaVersion: row.schema_version,
      promptVersion: row.prompt_version,
      provider: providerModel.provider,
      model: providerModel.model,
    };
  } catch {
    return {
      outcome: "retry",
      reason: "redis publish/ack failed after written transition",
      schemaVersion: row.schema_version,
      promptVersion: row.prompt_version,
      provider: providerModel.provider,
      model: providerModel.model,
    };
  }
}

/**
 * Consumes validated state_resources items and writes them into the production table.
 */
export async function runStateResourcesWriter(options: WriterOptions = {}): Promise<void> {
  const {
    once = false,
    batchSize = 20,
    blockMs = 5000,
    reclaimMinIdleMs = RECLAIM_MIN_IDLE_MS,
  } = options;

  const env = getPipelineEnv();
  const observer = createStageObserver("writer", {
    provider: env.AI_PROVIDER,
    model: env.AI_MODEL,
    prompt_version: env.STATE_RESOURCES_PROMPT_VERSION,
  });
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  const consumerName = `writer-${process.pid}`;
  let written = 0;
  let failed = 0;
  let skipped = 0;
  let retried = 0;
  let recovered = 0;

  const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
    for (const entry of entries) {
      const startedAtMs = Date.now();
      const ingestKey = entry.message.ingest_key ?? null;
      const eventRunId = normalizeRunId(entry.message.run_id);

      try {
        const result = await processMessage(pool, redis, entry.id, entry.message);
        const outcome = result.outcome;
        if (outcome === "written") {
          written += 1;
        } else if (outcome === "failed") {
          failed += 1;
        } else if (outcome === "retry") {
          retried += 1;
        } else if (outcome === "recovered") {
          recovered += 1;
        } else {
          skipped += 1;
        }

        observer.record({
          outcome,
          ingest_key: ingestKey,
          run_id: eventRunId,
          provider: result.provider,
          model: result.model,
          schema_version: result.schemaVersion,
          prompt_version: result.promptVersion,
          reason: result.reason,
          duration_ms: Date.now() - startedAtMs,
        });
      } catch (error) {
        const reason = toReason(error);

        if (!ingestKey) {
          failed += 1;
          await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, entry.id);
          observer.record({
            outcome: "failed",
            ingest_key: null,
            run_id: eventRunId,
            reason,
            duration_ms: Date.now() - startedAtMs,
          });
          continue;
        }

        const status = await getStagingStatus(pool, ingestKey);
        if (status === "validated") {
          failed += 1;
          const row = await getStagingRow(pool, ingestKey);
          const expectedRunId = normalizeRunId(entry.message.run_id) ?? normalizeRunId(row?.run_id ?? null);
          await markFailed(pool, ingestKey, reason, expectedRunId);
          await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, entry.id);
          observer.record({
            outcome: "failed",
            ingest_key: ingestKey,
            run_id: eventRunId,
            reason,
            duration_ms: Date.now() - startedAtMs,
          });
          continue;
        }

        if (status === "written") {
          // Keep unacked to allow XAUTOCLAIM recovery and re-publish of written event.
          retried += 1;
          observer.record({
            outcome: "retry",
            ingest_key: ingestKey,
            run_id: eventRunId,
            reason,
            duration_ms: Date.now() - startedAtMs,
          });
          continue;
        }

        failed += 1;
        await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, entry.id);
        observer.record({
          outcome: "failed",
          ingest_key: ingestKey,
          run_id: eventRunId,
          reason,
          duration_ms: Date.now() - startedAtMs,
        });
      }
    }
  };

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    let keepRunning = true;

    while (keepRunning) {
      const reclaimed = await reclaimPendingEntries(
        redis,
        consumerName,
        batchSize,
        reclaimMinIdleMs
      );
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_STATE_RESOURCES_WRITER_GROUP,
        consumerName,
        [{ key: STAGING_VALIDATED_STREAM, id: ">" }],
        { COUNT: batchSize, BLOCK: blockMs }
      );

      if (batches && batches.length > 0) {
        for (const batch of batches) {
          await handleEntries(batch.messages.map((entry) => ({ id: entry.id, message: entry.message })));
        }
      }

      if (once) {
        keepRunning = false;
      }
    }
  } finally {
    try {
      await redis.quit();
    } catch (error) {
      console.error("writer cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("writer cleanup warning (pool.end):", toReason(error));
    }

    observer.flush({ written, recovered, failed, skipped, retried });

    console.log(
      `state_resources writer completed. written=${written} recovered=${recovered} failed=${failed} skipped=${skipped} retried=${retried}`
    );
  }
}
