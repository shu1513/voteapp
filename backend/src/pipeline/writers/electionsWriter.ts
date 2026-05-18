import { Pool, type PoolClient } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_ELECTIONS_WRITER_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_VALIDATED_STREAM,
  STAGING_WRITTEN_STREAM,
} from "../../config/electionsPipeline.js";
import { parseCanonicalElectionPayload } from "../../contracts/electionPayloadContract.js";
import type { ElectionEnrichedPayload } from "../../types/election.js";

type WriterOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type StagingRow = {
  ingest_key: string;
  payload: unknown;
  status: string;
  run_id: string | null;
};

// Writing elections + downstream publish can take time; only reclaim clearly stale pending entries.
const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, "0", { MKSTREAM: true });
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
      SELECT ingest_key, payload, status, run_id
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_ELECTION]
  );
  return result.rows[0] ?? null;
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
      STAGING_VALIDATED_STREAM,
      STAGING_ELECTIONS_WRITER_GROUP,
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

async function writeElectionsForDistrict(
  client: PoolClient,
  ingestKey: string,
  payload: ElectionEnrichedPayload
): Promise<boolean> {
  await client.query("BEGIN");
  try {
    const statusUpdate = await client.query(
      `
        UPDATE staging_items
        SET status = 'written',
            reason = NULL,
            written_at = now(),
            updated_at = now()
        WHERE ingest_key = $1
          AND item_type = $2
          AND status = 'validated'
      `,
      [ingestKey, STAGING_ITEM_TYPE_ELECTION]
    );
    if (statusUpdate.rowCount !== 1) {
      await client.query("ROLLBACK");
      return false;
    }

    await client.query(`DELETE FROM public.elections WHERE district_id = $1`, [payload.district_id]);

    for (const entry of payload.entries) {
      await client.query(
        `
          INSERT INTO public.elections (
            district_id,
            official_ballot_title,
            description,
            election_date,
            race_type,
            sources
          ) VALUES ($1, $2, $3, $4::date, $5, $6::jsonb)
        `,
        [
          payload.district_id,
          entry.official_ballot_title,
          entry.description,
          entry.election_date,
          entry.race_type,
          JSON.stringify(entry.sources),
        ]
      );
    }

    await client.query("COMMIT");
    return true;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

export async function runElectionsWriter(options: WriterOptions = {}): Promise<void> {
  const { once = false, batchSize = 25, blockMs = 5000 } = options;
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const consumerName = `elections_writer_${process.pid}_${Date.now()}`;

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
      for (const entry of entries) {
        const ingestKey = entry.message.ingest_key;
        const itemType = entry.message.item_type;

        try {
          if (!ingestKey || itemType !== STAGING_ITEM_TYPE_ELECTION) {
            await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
            continue;
          }

          const row = await getStagingRow(pool, ingestKey);
          if (!row) {
            await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
            continue;
          }

          const parsed = parseCanonicalElectionPayload(row.payload);
          if (!parsed.ok) {
            await pool.query(
              `
                UPDATE staging_items
                SET status = 'failed',
                    reason = $2,
                    updated_at = now()
                WHERE ingest_key = $1
                  AND item_type = $3
              `,
              [ingestKey, `writer parse error: ${parsed.reason}`, STAGING_ITEM_TYPE_ELECTION]
            );
            await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
            continue;
          }

          if (row.status === "validated") {
            const client = await pool.connect();
            try {
              const wrote = await writeElectionsForDistrict(client, ingestKey, parsed.payload);
              if (!wrote) {
                await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
                continue;
              }
            } finally {
              client.release();
            }
          } else if (row.status !== "written") {
            await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
            continue;
          }

          // If DB is already written (including reclaimed post-commit failures), re-emit handoff and ack.
          await redis.xAdd(STAGING_WRITTEN_STREAM, "*", {
            ingest_key: ingestKey,
            item_type: STAGING_ITEM_TYPE_ELECTION,
            run_id: row.run_id ?? "",
            payload: JSON.stringify(parsed.payload),
          });
          await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
        } catch (error) {
          const reason = toReason(error);
          if (ingestKey) {
            console.warn(`elections writer retrying ingest_key=${ingestKey}: ${reason}`);
          } else {
            console.warn(`elections writer retrying message without ingest key: ${reason}`);
          }
          // Leave unacked; reclaim will pick it up.
        }
      }
    };

    do {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_ELECTIONS_WRITER_GROUP,
        consumerName,
        [{ key: STAGING_VALIDATED_STREAM, id: ">" }],
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
      console.error("elections writer cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("elections writer cleanup warning (pool.end):", toReason(error));
    }
  }
}
