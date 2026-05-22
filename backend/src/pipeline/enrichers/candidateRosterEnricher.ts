import { Pool } from "pg";
import { createClient } from "redis";

import {
  buildCandidateRosterConfigFromEnv,
  enrichCandidateRoster,
} from "../../ai/enrichCandidateRoster.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
  STAGING_CANDIDATE_ROSTER_REJECTED_STREAM,
  STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
  STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
  STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
  STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
} from "../../config/electionsPipeline.js";
import { normalizeCandidateName } from "../../utils/candidateIdentity.js";
import { parseCandidateRosterPayload, type CandidateRosterEntry } from "../../contracts/candidateRosterPayloadContract.js";

type EnricherOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type ElectionRow = {
  id: string;
  district_name: string;
  district_type: string;
  state: string;
  election_date: string;
  official_ballot_title: string;
  is_partisan: boolean | null;
  sources: unknown;
};

type CandidateRosterStagingRow = {
  ingest_key: string;
  payload: unknown;
  status: string;
  run_id: string | null;
};

const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const MAX_SEED_URLS = 8;
const MAX_DELIVERY_ATTEMPTS = 8;
const PROFILE_DRAFT_EMIT_MARKER_PREFIX = "staging:candidate_profile_draft_emitted:";
const CANDIDATE_ROSTER_STAGING_PREFIX = "candidate_roster:";

const EMIT_CANDIDATE_PROFILE_DRAFT_IF_NEEDED_LUA = `
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call(
  "XADD",
  KEYS[1],
  "*",
  "election_id",
  ARGV[1],
  "item_type",
  ARGV[2],
  "run_id",
  ARGV[3],
  "candidate_display_name",
  ARGV[4],
  "roster_party",
  ARGV[5],
  "roster_is_incumbent",
  ARGV[6],
  "seed_urls",
  ARGV[7],
  "emitted_at",
  ARGV[8]
)
redis.call("SET", KEYS[2], ARGV[8])
return 1
`;

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function parseSeedUrls(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  const urls: string[] = [];
  for (const item of raw) {
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

function mergeSeedUrls(...lists: Array<readonly string[] | undefined>): string[] {
  const merged: string[] = [];
  const seen = new Set<string>();
  for (const list of lists) {
    for (const item of list ?? []) {
      const trimmed = item.trim();
      if (trimmed.length === 0 || seen.has(trimmed)) {
        continue;
      }
      seen.add(trimmed);
      merged.push(trimmed);
      if (merged.length >= MAX_SEED_URLS) {
        return merged;
      }
    }
  }
  return merged;
}

function rosterIngestKeyForElection(electionId: string): string {
  return `${CANDIDATE_ROSTER_STAGING_PREFIX}${electionId}`;
}

function extractRosterCandidatesFromStagingPayload(payload: unknown): CandidateRosterEntry[] | null {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return null;
  }
  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.candidates)) {
    return null;
  }
  const parsed = parseCandidateRosterPayload({ candidates: input.candidates });
  if (!parsed.ok) {
    return null;
  }
  return parsed.payload.candidates;
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(
      STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
      STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
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
      STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
      STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
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
      STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
      STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
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
  await redis.xAdd(STAGING_CANDIDATE_ROSTER_REJECTED_STREAM, "*", {
    reason,
    delivery_count: deliveryCount === null ? "" : String(deliveryCount),
    original_stream_id: entry.id,
    election_id: entry.message.election_id ?? "",
    candidate_display_name: entry.message.candidate_display_name ?? "",
    item_type: entry.message.item_type ?? "",
    run_id: entry.message.run_id ?? "",
  });
  await redis.xAck(STAGING_CANDIDATE_ROSTER_DRAFT_STREAM, STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP, entry.id);
}

async function getCandidateRosterStagingRow(
  pool: Pool,
  ingestKey: string
): Promise<CandidateRosterStagingRow | null> {
  const result = await pool.query<CandidateRosterStagingRow>(
    `
      SELECT ingest_key, payload, status, run_id
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
      LIMIT 1
    `,
    [ingestKey, STAGING_ITEM_TYPE_CANDIDATE_ROSTER]
  );
  return result.rows[0] ?? null;
}

async function ensureCandidateRosterStagingRow(
  pool: Pool,
  ingestKey: string,
  electionId: string,
  runId: string | null
): Promise<void> {
  await pool.query(
    `
      INSERT INTO staging_items (
        ingest_key,
        item_type,
        payload,
        status,
        reason,
        run_id,
        model,
        schema_version,
        prompt_version
      )
      VALUES ($1, $2, jsonb_build_object('election_id', $3), 'pending', NULL, $4, NULL, NULL, NULL)
      ON CONFLICT (ingest_key) DO NOTHING
    `,
    [ingestKey, STAGING_ITEM_TYPE_CANDIDATE_ROSTER, electionId, runId ?? ""]
  );
}

async function markCandidateRosterStagingValidated(
  pool: Pool,
  ingestKey: string,
  electionId: string,
  candidates: CandidateRosterEntry[],
  runId: string | null,
  aiRawDebug: Record<string, unknown> | null
): Promise<void> {
  await pool.query(
    `
      UPDATE staging_items
      SET payload = jsonb_build_object('election_id', $2, 'candidates', $3::jsonb),
          status = 'validated',
          reason = NULL,
          failure_debug = NULL,
          ai_raw_debug = $4::jsonb,
          run_id = $5,
          validated_at = now(),
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $6
    `,
    [
      ingestKey,
      electionId,
      JSON.stringify(candidates),
      aiRawDebug ? JSON.stringify(aiRawDebug) : null,
      runId ?? "",
      STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
    ]
  );
}

async function markCandidateRosterStagingWritten(pool: Pool, ingestKey: string): Promise<void> {
  await pool.query(
    `
      UPDATE staging_items
      SET status = 'written',
          reason = NULL,
          written_at = now(),
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_CANDIDATE_ROSTER]
  );
}

async function recordCandidateRosterAiFailure(
  pool: Pool,
  ingestKey: string,
  reason: string,
  failureDebug: Record<string, unknown> | undefined
): Promise<void> {
  await pool.query(
    `
      UPDATE staging_items
      SET reason = $2,
          failure_debug = $3::jsonb,
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $4
    `,
    [
      ingestKey,
      reason,
      failureDebug ? JSON.stringify(failureDebug) : null,
      STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
    ]
  );
}

async function filterAlreadyLinkedCandidates(
  pool: Pool,
  electionId: string,
  candidates: CandidateRosterEntry[]
): Promise<CandidateRosterEntry[]> {
  if (candidates.length === 0) {
    return [];
  }

  const result = await pool.query<{ first_name: string; last_name: string }>(
    `
      SELECT c.first_name, c.last_name
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS c
        ON c.id = ce.candidate_id
      WHERE ce.election_id = $1
        AND c.deleted_at IS NULL
    `,
    [electionId]
  );

  const linkedNames = new Set(
    result.rows
      .map((row) => normalizeCandidateName(`${row.first_name} ${row.last_name}`))
      .filter((name) => name.length > 0)
  );

  return candidates.filter((candidate) => {
    const normalizedName = normalizeCandidateName(candidate.display_name);
    return normalizedName.length > 0 && !linkedNames.has(normalizedName);
  });
}

async function getElectionRow(pool: Pool, electionId: string): Promise<ElectionRow | null> {
  const result = await pool.query<ElectionRow>(
    `
      SELECT
        e.id,
        d.name AS district_name,
        d.district_type,
        d.state,
        e.election_date::text AS election_date,
        e.official_ballot_title,
        e.is_partisan,
        e.sources
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      WHERE e.id = $1
        AND e.race_type = 'office'
      LIMIT 1
    `,
    [electionId]
  );

  return result.rows[0] ?? null;
}

async function enqueueCandidateProfileDraft(
  redis: ReturnType<typeof createClient>,
  input: {
    electionId: string;
    runId: string | null;
    displayName: string;
    rosterParty?: string;
    rosterIsIncumbent?: boolean;
    seedUrls: readonly string[];
  }
): Promise<void> {
  const normalizedName = normalizeCandidateName(input.displayName);
  if (normalizedName.length === 0) {
    return;
  }

  const emittedAt = new Date().toISOString();
  const markerKey = `${PROFILE_DRAFT_EMIT_MARKER_PREFIX}${input.electionId}:${normalizedName}`;

  await redis.sendCommand([
    "EVAL",
    EMIT_CANDIDATE_PROFILE_DRAFT_IF_NEEDED_LUA,
    "2",
    STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
    markerKey,
    input.electionId,
    STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
    input.runId ?? "",
    input.displayName,
    input.rosterParty ?? "",
    input.rosterIsIncumbent === undefined ? "" : input.rosterIsIncumbent ? "true" : "false",
    JSON.stringify(input.seedUrls),
    emittedAt,
  ]);
}

export async function runCandidateRosterEnricher(options: EnricherOptions = {}): Promise<void> {
  const { once = false, batchSize = 25, blockMs = 5000 } = options;
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const consumerName = `candidate_roster_enricher_${process.pid}_${Date.now()}`;
  const aiConfig = buildCandidateRosterConfigFromEnv();

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
      for (const entry of entries) {
        const electionId = entry.message.election_id;
        const itemType = entry.message.item_type;
        const runId = entry.message.run_id ?? "";

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
              `candidate-roster enricher parked stream_id=${entry.id} election_id=${electionId ?? "unknown"} after ${deliveryCount} deliveries`
            );
            continue;
          }

          if (!electionId || itemType !== STAGING_ITEM_TYPE_CANDIDATE_ROSTER) {
            await redis.xAck(
              STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
              STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          const ingestKey = rosterIngestKeyForElection(electionId);
          await ensureCandidateRosterStagingRow(pool, ingestKey, electionId, runId);
          const stagingRow = await getCandidateRosterStagingRow(pool, ingestKey);
          if (!stagingRow) {
            await redis.xAck(
              STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
              STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          const election = await getElectionRow(pool, electionId);
          if (!election) {
            await redis.xAck(
              STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
              STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          let candidatesForFanout: CandidateRosterEntry[] | null = null;
          if (stagingRow.status === "validated" || stagingRow.status === "written") {
            candidatesForFanout = extractRosterCandidatesFromStagingPayload(stagingRow.payload) ?? [];
          } else {
            const aiResult = await enrichCandidateRoster(
              {
                districtName: election.district_name,
                districtType: election.district_type,
                state: election.state,
                electionDate: election.election_date,
                officialBallotTitle: election.official_ballot_title,
                electionIsPartisan: election.is_partisan,
                seedUrls: parseSeedUrls(election.sources),
              },
              aiConfig
            );

            if (!aiResult.ok) {
              await recordCandidateRosterAiFailure(
                pool,
                ingestKey,
                `ai failure (${aiResult.errorCode}): ${aiResult.reason}`,
                aiResult.failureDebug
              );
              console.warn(
                `candidate-roster enricher retrying election_id=${electionId}: ${aiResult.errorCode} ${aiResult.reason}`
              );
              // Leave unacked so reclaim retries.
              continue;
            }

            const filteredCandidates = await filterAlreadyLinkedCandidates(pool, electionId, aiResult.candidates);
            await markCandidateRosterStagingValidated(
              pool,
              ingestKey,
              electionId,
              filteredCandidates,
              runId || stagingRow.run_id,
              aiResult.aiRawDebug
            );
            candidatesForFanout = filteredCandidates;
          }

          const electionSeedUrls = parseSeedUrls(election.sources);
          for (const candidate of candidatesForFanout) {
            await enqueueCandidateProfileDraft(redis, {
              electionId,
              runId,
              displayName: candidate.display_name,
              rosterParty: candidate.party,
              rosterIsIncumbent: candidate.is_incumbent,
              seedUrls: mergeSeedUrls(candidate.sources, electionSeedUrls),
            });
          }

          await markCandidateRosterStagingWritten(pool, ingestKey);

          await redis.xAck(
            STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
            STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
            entry.id
          );
        } catch (error) {
          const reason = toReason(error);
          console.warn(`candidate-roster enricher retrying election_id=${electionId ?? "unknown"}: ${reason}`);
          // Leave unacked so reclaim retries.
        }
      }
    };

    do {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
        consumerName,
        [{ key: STAGING_CANDIDATE_ROSTER_DRAFT_STREAM, id: ">" }],
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
      console.error("candidate-roster enricher cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("candidate-roster enricher cleanup warning (pool.end):", toReason(error));
    }
  }
}
