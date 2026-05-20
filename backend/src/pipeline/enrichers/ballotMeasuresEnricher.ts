import { Pool } from "pg";
import { createClient } from "redis";

import {
  buildBallotMeasureAiConfigFromEnv,
  enrichBallotMeasure,
} from "../../ai/enrichBallotMeasure.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_BALLOT_MEASURE_DRAFT_STREAM,
  STAGING_BALLOT_MEASURE_ENRICHER_GROUP,
  STAGING_BALLOT_MEASURE_REJECTED_STREAM,
  STAGING_ITEM_TYPE_BALLOT_MEASURE,
} from "../../config/electionsPipeline.js";

type EnricherOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type ElectionRow = {
  id: string;
  district_id: string;
  district_name: string;
  district_type: string;
  state: string;
  election_date: string;
  official_ballot_title: string;
  sources: unknown;
};

const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const MAX_SEED_URLS = 5;
const MAX_DELIVERY_ATTEMPTS = 8;

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function parseSeedUrls(sources: unknown): string[] {
  if (!Array.isArray(sources)) {
    return [];
  }
  const urls: string[] = [];
  for (const item of sources) {
    if (typeof item !== "string") {
      continue;
    }
    const trimmed = item.trim();
    if (trimmed.length === 0) {
      continue;
    }
    urls.push(trimmed);
  }
  return [...new Set(urls)].slice(0, MAX_SEED_URLS);
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(
      STAGING_BALLOT_MEASURE_DRAFT_STREAM,
      STAGING_BALLOT_MEASURE_ENRICHER_GROUP,
      "0",
      { MKSTREAM: true }
    );
  } catch (error) {
    const message = toReason(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
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
      STAGING_BALLOT_MEASURE_DRAFT_STREAM,
      STAGING_BALLOT_MEASURE_ENRICHER_GROUP,
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

async function getDeliveryCount(
  redis: ReturnType<typeof createClient>,
  messageId: string
): Promise<number | null> {
  try {
    const raw = await redis.sendCommand([
      "XPENDING",
      STAGING_BALLOT_MEASURE_DRAFT_STREAM,
      STAGING_BALLOT_MEASURE_ENRICHER_GROUP,
      messageId,
      messageId,
      "1",
    ]);
    if (!Array.isArray(raw) || raw.length === 0) {
      return null;
    }
    const first = raw[0];
    if (!Array.isArray(first) || first.length < 4) {
      return null;
    }
    const deliveriesValue = first[3];
    const deliveries =
      typeof deliveriesValue === "number"
        ? deliveriesValue
        : Number.parseInt(String(deliveriesValue), 10);
    return Number.isFinite(deliveries) ? deliveries : null;
  } catch {
    return null;
  }
}

async function parkMessage(
  redis: ReturnType<typeof createClient>,
  entry: { id: string; message: Record<string, string> },
  reason: string,
  deliveryCount: number | null
): Promise<void> {
  await redis.xAdd(STAGING_BALLOT_MEASURE_REJECTED_STREAM, "*", {
    reason,
    delivery_count: deliveryCount === null ? "" : String(deliveryCount),
    original_stream_id: entry.id,
    election_id: entry.message.election_id ?? "",
    item_type: entry.message.item_type ?? "",
    run_id: entry.message.run_id ?? "",
  });
  await redis.xAck(STAGING_BALLOT_MEASURE_DRAFT_STREAM, STAGING_BALLOT_MEASURE_ENRICHER_GROUP, entry.id);
}

async function getElectionRow(pool: Pool, electionId: string): Promise<ElectionRow | null> {
  const result = await pool.query<ElectionRow>(
    `
      SELECT
        e.id,
        e.district_id,
        d.name AS district_name,
        d.district_type,
        d.state,
        e.election_date::text AS election_date,
        e.official_ballot_title,
        e.sources
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      WHERE e.id = $1
        AND e.race_type = 'ballot_measure'
      LIMIT 1
    `,
    [electionId]
  );
  return result.rows[0] ?? null;
}

async function ballotMeasureExists(pool: Pool, electionId: string): Promise<boolean> {
  const result = await pool.query<{ exists: boolean }>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM public.ballot_measures
        WHERE election_id = $1
      ) AS exists
    `,
    [electionId]
  );
  return result.rows[0]?.exists === true;
}

export async function runBallotMeasuresEnricher(options: EnricherOptions = {}): Promise<void> {
  const { once = false, batchSize = 25, blockMs = 5000 } = options;
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const consumerName = `ballot_measures_enricher_${process.pid}_${Date.now()}`;
  const aiConfig = buildBallotMeasureAiConfigFromEnv();

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
      for (const entry of entries) {
        const electionId = entry.message.election_id;
        const itemType = entry.message.item_type;

        try {
          const deliveryCount = await getDeliveryCount(redis, entry.id);
          if (deliveryCount !== null && deliveryCount >= MAX_DELIVERY_ATTEMPTS) {
            await parkMessage(
              redis,
              entry,
              `max delivery attempts exceeded (${MAX_DELIVERY_ATTEMPTS})`,
              deliveryCount
            );
            console.warn(
              `ballot-measures enricher parked stream_id=${entry.id} election_id=${electionId ?? "unknown"} after ${deliveryCount} deliveries`
            );
            continue;
          }

          if (!electionId || itemType !== STAGING_ITEM_TYPE_BALLOT_MEASURE) {
            await redis.xAck(STAGING_BALLOT_MEASURE_DRAFT_STREAM, STAGING_BALLOT_MEASURE_ENRICHER_GROUP, entry.id);
            continue;
          }

          if (await ballotMeasureExists(pool, electionId)) {
            await redis.xAck(STAGING_BALLOT_MEASURE_DRAFT_STREAM, STAGING_BALLOT_MEASURE_ENRICHER_GROUP, entry.id);
            continue;
          }

          const election = await getElectionRow(pool, electionId);
          if (!election) {
            await redis.xAck(STAGING_BALLOT_MEASURE_DRAFT_STREAM, STAGING_BALLOT_MEASURE_ENRICHER_GROUP, entry.id);
            continue;
          }

          const aiResult = await enrichBallotMeasure(
            {
              districtName: election.district_name,
              districtType: election.district_type,
              state: election.state,
              electionDate: election.election_date,
              officialBallotTitle: election.official_ballot_title,
              seedUrls: parseSeedUrls(election.sources),
            },
            aiConfig
          );

          if (!aiResult.ok) {
            console.warn(
              `ballot-measures enricher skipped election_id=${electionId} due to AI failure: ${aiResult.reason}`
            );
            // Keep unacked so reclaim can retry later.
            continue;
          }

          await pool.query(
            `
              INSERT INTO public.ballot_measures (
                district_id,
                election_id,
                official_ballot_title,
                what_yes_means,
                what_no_means,
                result,
                source_url,
                official_measure_url,
                last_researched
              )
              VALUES ($1, $2, $3, $4, $5, NULL, $6::jsonb, $7, now())
              ON CONFLICT (election_id) DO NOTHING
            `,
            [
              election.district_id,
              election.id,
              election.official_ballot_title,
              aiResult.whatYesMeans,
              aiResult.whatNoMeans,
              JSON.stringify(aiResult.researchUrls),
              aiResult.officialMeasureUrl,
            ]
          );

          await redis.xAck(STAGING_BALLOT_MEASURE_DRAFT_STREAM, STAGING_BALLOT_MEASURE_ENRICHER_GROUP, entry.id);
        } catch (error) {
          const reason = toReason(error);
          console.warn(`ballot-measures enricher retrying election_id=${electionId ?? "unknown"}: ${reason}`);
          // Leave unacked; reclaim will retry.
        }
      }
    };

    do {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_BALLOT_MEASURE_ENRICHER_GROUP,
        consumerName,
        [{ key: STAGING_BALLOT_MEASURE_DRAFT_STREAM, id: ">" }],
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
      console.error("ballot-measures enricher cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("ballot-measures enricher cleanup warning (pool.end):", toReason(error));
    }
  }
}
