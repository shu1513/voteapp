import { Pool } from "pg";
import { createClient } from "redis";

import {
  buildEnrichElectionsConfigFromEnv,
  enrichElections,
} from "../../ai/enrichElections.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_DRAFT_STREAM,
  STAGING_ELECTIONS_ENRICHER_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_PENDING_STREAM,
} from "../../config/electionsPipeline.js";
import {
  ELECTION_DRAFT_SCHEMA_VERSION,
  ELECTION_ENRICHMENT_SCHEMA_VERSION,
  ELECTION_PROMPT_VERSION,
} from "../../contracts/electionEnrichmentContract.js";
import type { ElectionDraftPayload } from "../../types/election.js";

type EnricherOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type StagingRow = {
  ingest_key: string;
  payload: unknown;
  status: string;
  run_id: string | null;
  schema_version: string | null;
  reason: string | null;
  failure_debug: unknown;
};

// Provider calls can take tens of seconds; keep reclaim idle window above normal processing time.
const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseSoftRetryCount(failureDebug: unknown): number {
  if (!isObjectRecord(failureDebug)) {
    return 0;
  }
  const raw = failureDebug.soft_retry_count;
  return typeof raw === "number" && Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : 0;
}

function parseReviewFeedback(failureDebug: unknown): string[] {
  if (!isObjectRecord(failureDebug)) {
    return [];
  }
  const raw = failureDebug.validation_feedback;
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.filter((item): item is string => typeof item === "string" && item.trim().length > 0).map((item) => item.trim());
}

function parseDraftPayload(payload: unknown): ElectionDraftPayload | null {
  if (!isObjectRecord(payload)) {
    return null;
  }
  const input = payload as Record<string, unknown>;
  if (
    typeof input.district_id !== "string" ||
    typeof input.district_name !== "string" ||
    typeof input.district_type !== "string" ||
    typeof input.state !== "string"
  ) {
    return null;
  }
  return {
    district_id: input.district_id.trim(),
    district_name: input.district_name.trim(),
    district_type: input.district_type as ElectionDraftPayload["district_type"],
    state: input.state.trim(),
  };
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, "0", { MKSTREAM: true });
  } catch (error) {
    const message = toReason(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

async function getStagingRow(pool: Pool, ingestKey: string): Promise<StagingRow | null> {
  const result = await pool.query<StagingRow>(
    `
      SELECT ingest_key, payload, status, run_id, schema_version, reason, failure_debug
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_ELECTION]
  );
  return result.rows[0] ?? null;
}

async function getStagingStatus(pool: Pool, ingestKey: string): Promise<string | null> {
  const result = await pool.query<{ status: string }>(
    `
      SELECT status
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_ELECTION]
  );
  return result.rows[0]?.status ?? null;
}

async function reclaimPendingEntries(
  redis: ReturnType<typeof createClient>,
  consumerName: string,
  batchSize: number
): Promise<Array<{ id: string; message: Record<string, string> }>> {
  const reclaimed: Array<{ id: string; message: Record<string, string> }> = [];
  let cursor = "0-0";

  for (let i = 0; i < RECLAIM_MAX_BATCHES; i += 1) {
    const claim = await redis.xAutoClaim(
      STAGING_DRAFT_STREAM,
      STAGING_ELECTIONS_ENRICHER_GROUP,
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

export async function runElectionsEnricher(options: EnricherOptions = {}): Promise<void> {
  const { once = false, batchSize = 25, blockMs = 5000 } = options;
  const env = getPipelineEnv();
  const config = buildEnrichElectionsConfigFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const consumerName = `elections_enricher_${process.pid}_${Date.now()}`;

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
      for (const entry of entries) {
        const ingestKey = entry.message.ingest_key;
        const itemType = entry.message.item_type;

        try {
          if (!ingestKey || itemType !== STAGING_ITEM_TYPE_ELECTION) {
            await redis.xAck(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, entry.id);
            continue;
          }

          const row = await getStagingRow(pool, ingestKey);
          if (!row || row.status !== "pending" || row.schema_version !== ELECTION_DRAFT_SCHEMA_VERSION) {
            await redis.xAck(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, entry.id);
            continue;
          }

          const draft = parseDraftPayload(row.payload);
          if (!draft) {
            await pool.query(
              `
                UPDATE staging_items
                SET status = 'failed',
                    reason = $2,
                    updated_at = now()
                WHERE ingest_key = $1
                  AND item_type = $3
              `,
              [ingestKey, "invalid election draft payload shape", STAGING_ITEM_TYPE_ELECTION]
            );
            await redis.xAck(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, entry.id);
            continue;
          }

          const softRetryCount = parseSoftRetryCount(row.failure_debug);
          const reviewFeedback = parseReviewFeedback(row.failure_debug);
          const result = await enrichElections(
            {
              ingestKey,
              draft,
              promptVersion: ELECTION_PROMPT_VERSION,
              softRetryCount,
              reviewFeedback,
            },
            config
          );

          if (!result.ok) {
            await pool.query(
              `
                UPDATE staging_items
                SET status = 'failed',
                    reason = $2,
                    failure_debug = $3::jsonb,
                    updated_at = now()
                WHERE ingest_key = $1
                  AND item_type = $4
              `,
              [
                ingestKey,
                result.reason,
                JSON.stringify(result.failureDebug ?? {}),
                STAGING_ITEM_TYPE_ELECTION,
              ]
            );
            await redis.xAck(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, entry.id);
            continue;
          }

          await pool.query(
            `
              UPDATE staging_items
              SET payload = $2::jsonb,
                  schema_version = $3,
                  model = $4,
                  prompt_version = $5,
                  reason = NULL,
                  failure_debug = NULL,
                  ai_raw_debug = $6::jsonb,
                  status = 'pending',
                  updated_at = now()
              WHERE ingest_key = $1
                AND item_type = $7
            `,
            [
              ingestKey,
              JSON.stringify(result.payload),
              ELECTION_ENRICHMENT_SCHEMA_VERSION,
              `${result.provider}:${result.model}`,
              result.promptVersion,
              JSON.stringify(result.aiRawDebug ?? {}),
              STAGING_ITEM_TYPE_ELECTION,
            ]
          );

          await redis.xAdd(STAGING_PENDING_STREAM, "*", {
            ingest_key: ingestKey,
            item_type: STAGING_ITEM_TYPE_ELECTION,
            run_id: row.run_id ?? "",
            payload: JSON.stringify(result.payload),
          });
          await redis.xAck(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, entry.id);
        } catch (error) {
          const reason = toReason(error);
          if (!ingestKey) {
            try {
              await redis.xAck(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, entry.id);
            } catch {
              // keep unacked when ack fails; reclaim pass will retry later
            }
            console.error(`elections enricher message error (missing ingest key): ${reason}`);
            continue;
          }

          const status = await getStagingStatus(pool, ingestKey);
          if (status === "pending") {
            // leave unacked so reclaim pass can retry
            console.warn(`elections enricher retrying ingest_key=${ingestKey}: ${reason}`);
            continue;
          }

          try {
            await redis.xAck(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, entry.id);
          } catch {
            // keep unacked when ack fails; reclaim pass will retry later
          }
          console.error(`elections enricher message error ingest_key=${ingestKey}: ${reason}`);
        }
      }
    };

    do {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_ELECTIONS_ENRICHER_GROUP,
        consumerName,
        [{ key: STAGING_DRAFT_STREAM, id: ">" }],
        { COUNT: batchSize, BLOCK: blockMs }
      );

      if (!batches || batches.length === 0) {
        if (once) {
          break;
        }
        continue;
      }

      for (const batch of batches) {
        await handleEntries(
          batch.messages.map((message) => ({
            id: message.id,
            message: message.message as Record<string, string>,
          }))
        );
      }

      if (once) {
        break;
      }
    } while (true);
  } finally {
    try {
      await redis.quit();
    } catch (error) {
      console.error("elections enricher cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("elections enricher cleanup warning (pool.end):", toReason(error));
    }
  }
}
