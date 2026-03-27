import { Pool, type PoolClient } from "pg";
import { createClient } from "redis";

import {
  STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
  STATE_RESOURCE_REQUIRED_TEXT_FIELDS,
  STATE_RESOURCE_SOURCE_FIELDS,
} from "../../contracts/stateResourceEnrichmentContract.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STAGING_STATE_RESOURCES_WRITER_GROUP,
  STAGING_VALIDATED_STREAM,
  STAGING_WRITTEN_STREAM,
} from "../../config/stateResourcePipeline.js";
import type { SourceCitation, StateResourcePayload, StateResourceSources } from "../../types/stateResource.js";
import { createStageObserver } from "../utils/observability.js";
import { hasRunIdMismatch, normalizeRunId } from "../utils/runIdGuard.js";

type WriterOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type StagingRow = {
  ingest_key: string;
  item_type: string;
  run_id: string | null;
  schema_version: string | null;
  prompt_version: string | null;
  reason: string | null;
  payload: unknown;
  status: string;
};

type WriterOutcome = "written" | "failed" | "skipped" | "retry" | "recovered";

type ParseResult =
  | { ok: true; payload: StateResourcePayload }
  | { ok: false; reason: string };

const RECLAIM_MIN_IDLE_MS = 30_000;
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

/**
 * Returns true when one source citation object has the required fields.
 */
function isValidCitation(value: unknown): value is SourceCitation {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const item = value as Record<string, unknown>;
  return isNonEmptyString(item.source_url) && isNonEmptyString(item.source_name);
}

/**
 * Parses a validated staging payload into the strict StateResourcePayload shape.
 */
function parseStateResourcePayload(payload: unknown): ParseResult {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  for (const key of STATE_RESOURCE_REQUIRED_TEXT_FIELDS) {
    if (!isNonEmptyString(input[key])) {
      return { ok: false, reason: `payload.${key} must be a non-empty string` };
    }
  }

  if (typeof input.sources !== "object" || input.sources === null || Array.isArray(input.sources)) {
    return { ok: false, reason: "payload.sources must be an object" };
  }

  const sources = input.sources as Record<string, unknown>;
  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const citations = sources[key];
    if (!Array.isArray(citations) || citations.length === 0) {
      return { ok: false, reason: `payload.sources.${key} must be a non-empty array` };
    }

    if (!citations.every(isValidCitation)) {
      return { ok: false, reason: `payload.sources.${key} contains invalid citation entries` };
    }
  }

  return {
    ok: true,
    payload: {
      state_fips: (input.state_fips as string).trim(),
      state_abbreviation: (input.state_abbreviation as string).trim(),
      state_name: (input.state_name as string).trim(),
      polling_place_url: (input.polling_place_url as string).trim(),
      voter_registration_url: (input.voter_registration_url as string).trim(),
      vote_by_mail_info: (input.vote_by_mail_info as string).trim(),
      polling_hours: (input.polling_hours as string).trim(),
      id_requirements: (input.id_requirements as string).trim(),
      sources: sources as StateResourceSources,
    },
  };
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
      SELECT ingest_key, item_type, run_id, schema_version, prompt_version, reason, payload, status
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
          vote_by_mail_info,
          polling_hours,
          id_requirements,
          sources
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb
        )
        ON CONFLICT (state_fips) DO UPDATE SET
          state_abbreviation = EXCLUDED.state_abbreviation,
          state_name = EXCLUDED.state_name,
          polling_place_url = EXCLUDED.polling_place_url,
          voter_registration_url = EXCLUDED.voter_registration_url,
          vote_by_mail_info = EXCLUDED.vote_by_mail_info,
          polling_hours = EXCLUDED.polling_hours,
          id_requirements = EXCLUDED.id_requirements,
          sources = EXCLUDED.sources
      `,
      [
        payload.state_fips,
        payload.state_abbreviation,
        payload.state_name,
        payload.polling_place_url,
        payload.voter_registration_url,
        payload.vote_by_mail_info,
        payload.polling_hours,
        payload.id_requirements,
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
  batchSize: number
): Promise<Array<{ id: string; message: Record<string, string> }>> {
  const reclaimed: Array<{ id: string; message: Record<string, string> }> = [];
  let cursor = "0-0";

  for (let i = 0; i < RECLAIM_MAX_BATCHES; i += 1) {
    const claim = await redis.xAutoClaim(
      STAGING_VALIDATED_STREAM,
      STAGING_STATE_RESOURCES_WRITER_GROUP,
      consumerName,
      RECLAIM_MIN_IDLE_MS,
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
): Promise<WriterOutcome> {
  const ingestKey = message.ingest_key;
  const messageRunId = normalizeRunId(message.run_id);

  if (!ingestKey) {
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return "skipped";
  }

  const row = await getStagingRow(pool, ingestKey);

  if (!row) {
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return "skipped";
  }

  if (hasRunIdMismatch(message.run_id, row.run_id)) {
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return "skipped";
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
      return "recovered";
    } catch {
      // Leave unacked so it can be reclaimed and retried.
      return "retry";
    }
  }

  if (row.status !== "validated") {
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return "skipped";
  }

  if (!isNonEmptyString(row.prompt_version)) {
    await markFailed(pool, ingestKey, "prompt_version metadata is required", expectedRunId);
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return "failed";
  }

  if (row.schema_version !== STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION) {
    await markFailed(
      pool,
      ingestKey,
      `schema_version must be ${STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION} for writer input`,
      expectedRunId
    );
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return "failed";
  }

  const parsed = parseStateResourcePayload(row.payload);
  if (!parsed.ok) {
    await markFailed(pool, ingestKey, parsed.reason, expectedRunId);
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return "failed";
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
      return "skipped";
    }
  } catch (error) {
    const reason = toReason(error);
    await markFailed(pool, ingestKey, reason, expectedRunId);
    await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_STATE_RESOURCES_WRITER_GROUP, messageId);
    return "failed";
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
    return "written";
  } catch {
    return "retry";
  }
}

/**
 * Consumes validated state_resources items and writes them into the production table.
 */
export async function runStateResourcesWriter(options: WriterOptions = {}): Promise<void> {
  const { once = false, batchSize = 20, blockMs = 5000 } = options;

  const env = getPipelineEnv();
  const observer = createStageObserver("writer", {
    provider: env.AI_PROVIDER,
    model: env.AI_MODEL,
    prompt_version: env.PROMPT_VERSION,
  });
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  await redis.connect();
  await ensureConsumerGroup(redis);

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
        const outcome = await processMessage(pool, redis, entry.id, entry.message);
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

        let reason: string | null = null;
        let schemaVersion: string | null = null;
        let promptVersion: string | null = null;
        if (ingestKey && outcome !== "skipped") {
          const row = await getStagingRow(pool, ingestKey);
          reason = row?.reason ?? null;
          schemaVersion = row?.schema_version ?? null;
          promptVersion = row?.prompt_version ?? null;
        }

        observer.record({
          outcome,
          ingest_key: ingestKey,
          run_id: eventRunId,
          schema_version: schemaVersion,
          prompt_version: promptVersion,
          reason,
          duration_ms: Date.now() - startedAtMs,
        });
      } catch (error) {
        failed += 1;
        const reason = toReason(error);

        if (!ingestKey) {
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
    let keepRunning = true;

    while (keepRunning) {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
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
    await redis.quit();
    await pool.end();
  }

  observer.flush({ written, recovered, failed, skipped, retried });

  console.log(
    `state_resources writer completed. written=${written} recovered=${recovered} failed=${failed} skipped=${skipped} retried=${retried}`
  );
}
