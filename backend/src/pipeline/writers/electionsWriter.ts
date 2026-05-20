import { Pool, type PoolClient } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_BALLOT_MEASURE_DRAFT_STREAM,
  STAGING_ITEM_TYPE_BALLOT_MEASURE,
  STAGING_ELECTIONS_WRITER_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_VALIDATED_STREAM,
  STAGING_WRITTEN_STREAM,
} from "../../config/electionsPipeline.js";
import { parseCanonicalElectionPayload } from "../../contracts/electionPayloadContract.js";
import type { ElectionEnrichedPayload } from "../../types/election.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";
import type { ElectionContestFamily } from "../../ai/providers/electionsPrompt.js";

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
  ai_raw_debug: unknown;
};

type WriteResult = {
  wrote: boolean;
  ballotMeasureElectionIds: string[];
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
           , ai_raw_debug
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

function extractFamilySeedUrls(aiRawDebug: unknown): Partial<Record<ElectionContestFamily, string[]>> {
  if (typeof aiRawDebug !== "object" || aiRawDebug === null || Array.isArray(aiRawDebug)) {
    return {};
  }
  const record = aiRawDebug as Record<string, unknown>;
  const raw = record.family_source_urls;
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return {};
  }

  const families: ElectionContestFamily[] = [
    "all",
    "non_judicial_office",
    "judicial_office",
    "ballot_measure",
  ];
  const result: Partial<Record<ElectionContestFamily, string[]>> = {};
  const sourceRecord = raw as Record<string, unknown>;

  for (const family of families) {
    const list = sourceRecord[family];
    if (!Array.isArray(list) || list.length === 0) {
      continue;
    }
    const urls = [
      ...new Set(
        list
          .filter((item): item is string => typeof item === "string")
          .map((item) => normalizeHttpUrl(item))
          .filter((item): item is string => Boolean(item))
      ),
    ];
    if (urls.length > 0) {
      result[family] = urls;
    }
  }

  return result;
}

async function resolveBallotMeasureElectionIds(
  pool: Pool,
  payload: ElectionEnrichedPayload
): Promise<string[]> {
  const ballotEntries = payload.entries.filter((entry) => entry.race_type === "ballot_measure");
  if (ballotEntries.length === 0) {
    return [];
  }

  const titles = ballotEntries.map((entry) => entry.official_ballot_title);
  const dates = ballotEntries.map((entry) => entry.election_date);

  const result = await pool.query<{ id: string }>(
    `
      SELECT e.id
      FROM public.elections AS e
      JOIN unnest($2::text[], $3::date[]) AS m(official_ballot_title, election_date)
        ON e.official_ballot_title = m.official_ballot_title
       AND e.election_date = m.election_date
      WHERE e.district_id = $1
    `,
    [payload.district_id, titles, dates]
  );

  return [...new Set(result.rows.map((row) => row.id))];
}

async function enqueueBallotMeasureDrafts(
  redis: ReturnType<typeof createClient>,
  electionIds: readonly string[],
  runId: string | null
): Promise<void> {
  const uniqueElectionIds = [...new Set(electionIds)];
  for (const electionId of uniqueElectionIds) {
    await redis.xAdd(STAGING_BALLOT_MEASURE_DRAFT_STREAM, "*", {
      election_id: electionId,
      item_type: STAGING_ITEM_TYPE_BALLOT_MEASURE,
      run_id: runId ?? "",
    });
  }
}

async function writeElectionsForDistrict(
  client: PoolClient,
  ingestKey: string,
  payload: ElectionEnrichedPayload,
  familySeedUrls: Partial<Record<ElectionContestFamily, string[]>>
): Promise<WriteResult> {
  await client.query("BEGIN");
  try {
    const nextStatus = payload.entries.length === 0 ? "no_results" : "written";
    const statusUpdate = await client.query(
      `
        UPDATE staging_items
        SET status = $3,
            reason = NULL,
            written_at = now(),
            updated_at = now()
        WHERE ingest_key = $1
          AND item_type = $2
          AND status = 'validated'
      `,
      [ingestKey, STAGING_ITEM_TYPE_ELECTION, nextStatus]
    );
    if (statusUpdate.rowCount !== 1) {
      await client.query("ROLLBACK");
      return { wrote: false, ballotMeasureElectionIds: [] };
    }

    const ballotMeasureElectionIds: string[] = [];
    for (const entry of payload.entries) {
      const upsertResult = await client.query<{ id: string; race_type: string }>(
        `
          INSERT INTO public.elections (
            district_id,
            official_ballot_title,
            description,
            election_date,
            race_type,
            election_stage,
            sources
          ) VALUES ($1, $2, $3, $4::date, $5, $6, $7::jsonb)
          ON CONFLICT (district_id, official_ballot_title, election_date) DO UPDATE SET
            description = EXCLUDED.description,
            race_type = EXCLUDED.race_type,
            election_stage = COALESCE(EXCLUDED.election_stage, elections.election_stage),
            sources = EXCLUDED.sources,
            updated_at = now()
          RETURNING id, race_type
        `,
        [
          payload.district_id,
          entry.official_ballot_title,
          entry.description,
          entry.election_date,
          entry.race_type,
          entry.election_stage ?? null,
          JSON.stringify(entry.sources),
        ]
      );
      const row = upsertResult.rows?.[0];
      if (row?.race_type === "ballot_measure") {
        ballotMeasureElectionIds.push(row.id);
      }
    }

    const seedRows: Array<{ family: string; url: string }> = [];
    const seenSeedKeys = new Set<string>();
    for (const [family, urls] of Object.entries(familySeedUrls)) {
      for (const url of urls ?? []) {
        const key = `${family}::${url}`;
        if (seenSeedKeys.has(key)) {
          continue;
        }
        seenSeedKeys.add(key);
        seedRows.push({ family, url });
      }
    }

    if (seedRows.length > 0) {
      await client.query(
        `
          INSERT INTO election_seed_urls (district_id, contest_family, url, last_seen_at)
          SELECT
            $1::uuid,
            seed.contest_family,
            seed.url,
            now()
          FROM unnest($2::text[], $3::text[]) AS seed(contest_family, url)
          ON CONFLICT (district_id, contest_family, url) DO UPDATE
          SET last_seen_at = EXCLUDED.last_seen_at,
              updated_at = now()
        `,
        [
          payload.district_id,
          seedRows.map((row) => row.family),
          seedRows.map((row) => row.url),
        ]
      );
    }

    await client.query("COMMIT");
    return {
      wrote: true,
      ballotMeasureElectionIds,
    };
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
            const familySeedUrls = extractFamilySeedUrls(row.ai_raw_debug);
            let ballotMeasureElectionIds: string[] = [];
            const client = await pool.connect();
            try {
              const writeResult = await writeElectionsForDistrict(
                client,
                ingestKey,
                parsed.payload,
                familySeedUrls
              );
              if (!writeResult.wrote) {
                await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
                continue;
              }
              ballotMeasureElectionIds = writeResult.ballotMeasureElectionIds;
            } finally {
              client.release();
            }
            await enqueueBallotMeasureDrafts(redis, ballotMeasureElectionIds, row.run_id);
          } else if (row.status !== "written" && row.status !== "no_results") {
            await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
            continue;
          } else {
            const ballotMeasureElectionIds = await resolveBallotMeasureElectionIds(pool, parsed.payload);
            await enqueueBallotMeasureDrafts(redis, ballotMeasureElectionIds, row.run_id);
          }

          // If DB is already persisted (including reclaimed post-commit failures), re-emit handoff and ack.
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
