import { Pool, type PoolClient } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_BALLOT_MEASURE_DRAFT_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
  STAGING_ITEM_TYPE_BALLOT_MEASURE,
  STAGING_ELECTIONS_WRITER_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_VALIDATED_STREAM,
  STAGING_WRITTEN_STREAM,
} from "../../config/electionsPipeline.js";
import { parseCanonicalElectionPayload } from "../../contracts/electionPayloadContract.js";
import type { ElectionEnrichedPayload } from "../../types/election.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";
import { normalizeElectionTitleKey } from "../../utils/normalizeElectionTitleKey.js";
import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";
import type { ElectionContestFamily } from "../../ai/providers/electionsPrompt.js";
import { enqueueCandidateRosterDrafts } from "../candidates/candidateRosterDraftEmitter.js";
import {
  defaultOfficeCandidateEligibilityConfig,
  evaluateOfficeCandidateEligibilityByElectionIds,
  summarizeOfficeCandidateEligibilityReasons,
} from "../candidates/officeCandidateEligibility.js";
import { OfficeMatcher } from "../elections/officeMatcher.js";

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
  officeElectionIds: string[];
};

// Writing elections + downstream publish can take time; only reclaim clearly stale pending entries.
const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const BALLOT_MEASURE_EMIT_MARKER_PREFIX = "staging:ballot_measure_emitted:";
const ENABLE_CANDIDATE_ROSTER_WRITER_ELIGIBILITY_FILTER =
  process.env.CANDIDATE_ROSTER_ENABLE_WRITER_ELIGIBILITY_FILTER === "true";
const EMIT_BALLOT_MEASURE_DRAFT_IF_NEEDED_LUA = `
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
  ARGV[3]
)
redis.call("SET", KEYS[2], ARGV[4])
return 1
`;

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
    "us_senate",
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

  const titleKeys = ballotEntries.map((entry) => normalizeElectionTitleKey(entry.official_ballot_title));
  const dates = ballotEntries.map((entry) => entry.election_date);

  const result = await pool.query<{ id: string }>(
    `
      SELECT e.id
      FROM public.elections AS e
      JOIN unnest($2::text[], $3::date[]) AS m(official_ballot_title_key, election_date)
        ON e.official_ballot_title_key = m.official_ballot_title_key
       AND e.election_date = m.election_date
      WHERE e.district_id = $1
        AND e.race_type = 'ballot_measure'
    `,
    [payload.district_id, titleKeys, dates]
  );

  return [...new Set(result.rows.map((row) => row.id))];
}

async function resolveOfficeElectionIds(
  pool: Pool,
  payload: ElectionEnrichedPayload
): Promise<string[]> {
  const officeEntries = payload.entries.filter((entry) => entry.race_type === "office");
  if (officeEntries.length === 0) {
    return [];
  }

  const titleKeys = officeEntries.map((entry) => normalizeElectionTitleKey(entry.official_ballot_title));
  const dates = officeEntries.map((entry) => entry.election_date);

  const result = await pool.query<{ id: string }>(
    `
      SELECT e.id
      FROM public.elections AS e
      JOIN unnest($2::text[], $3::date[]) AS o(official_ballot_title_key, election_date)
        ON e.official_ballot_title_key = o.official_ballot_title_key
       AND e.election_date = o.election_date
      WHERE e.district_id = $1
        AND e.race_type = 'office'
    `,
    [payload.district_id, titleKeys, dates]
  );

  return [...new Set(result.rows.map((row) => row.id))];
}

async function enqueueBallotMeasureDrafts(
  redis: ReturnType<typeof createClient>,
  electionIds: readonly string[],
  runId: string | null
): Promise<void> {
  const emittedAt = new Date().toISOString();
  const uniqueElectionIds = [...new Set(electionIds)];
  for (const electionId of uniqueElectionIds) {
    const markerKey = `${BALLOT_MEASURE_EMIT_MARKER_PREFIX}${electionId}`;
    await redis.sendCommand([
      "EVAL",
      EMIT_BALLOT_MEASURE_DRAFT_IF_NEEDED_LUA,
      "2",
      STAGING_BALLOT_MEASURE_DRAFT_STREAM,
      markerKey,
      electionId,
      STAGING_ITEM_TYPE_BALLOT_MEASURE,
      runId ?? "",
      emittedAt,
    ]);
  }
}

async function selectWriterEligibleOfficeElectionIds(
  pool: Pool,
  officeElectionIds: readonly string[],
  context: string
): Promise<string[]> {
  if (!ENABLE_CANDIDATE_ROSTER_WRITER_ELIGIBILITY_FILTER || officeElectionIds.length === 0) {
    return [...new Set(officeElectionIds)];
  }

  const config = defaultOfficeCandidateEligibilityConfig();
  const rows = await evaluateOfficeCandidateEligibilityByElectionIds(pool, officeElectionIds, config);
  const counts = summarizeOfficeCandidateEligibilityReasons(rows);
  const eligibleIds = rows.filter((row) => row.reason === "eligible").map((row) => row.election_id);

  console.log(
    `candidate-roster writer eligibility (${context}) as_of=${config.asOfDate}: ` +
      `input=${officeElectionIds.length} eligible=${eligibleIds.length} ` +
      `already_written=${counts.already_written} not_nearest=${counts.not_nearest_in_track} ` +
      `buffer_blocked=${counts.buffer_not_elapsed} too_far_in_future=${counts.too_far_in_future} ` +
      `not_upcoming=${counts.not_upcoming}`
  );

  return eligibleIds;
}

async function writeElectionsForDistrict(
  client: PoolClient,
  ingestKey: string,
  payload: ElectionEnrichedPayload,
  familySeedUrls: Partial<Record<ElectionContestFamily, string[]>>,
  runId: string | null
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
      return { wrote: false, ballotMeasureElectionIds: [], officeElectionIds: [] };
    }

    await client.query(
      `
        UPDATE public.districts
        SET last_elections_searched_at = now()
        WHERE id = $1
      `,
      [payload.district_id]
    );

    const ballotMeasureElectionIds: string[] = [];
    const officeElectionIds: string[] = [];
    const officeMatcher = new OfficeMatcher(client);
    const officeMatchCounts: Record<"alias_exact" | "deterministic_fallback" | "none" | "ambiguous", number> = {
      alias_exact: 0,
      deterministic_fallback: 0,
      none: 0,
      ambiguous: 0,
    };
    const unresolvedOfficeMatches: Array<{
      method: "none" | "ambiguous";
      confidence: number;
      officialBallotTitle: string;
      normalizedAlias: string;
    }> = [];
    const aliasRowsToInsert: Array<{
      office_id: string;
      scope: string;
      alias_text: string;
      normalized_alias: string;
    }> = [];
    const seenAliasKeys = new Set<string>();
    const senateMetadataRows: Array<{
      election_id: string;
      senate_class: string | null;
      term_end_year: string | null;
    }> = [];
    for (const entry of payload.entries) {
      let matchedOfficeId: string | null = null;
      if (entry.race_type === "office") {
        const officeMatch = await officeMatcher.resolve({
          scope: payload.district_type,
          districtName: payload.district_name,
          state: payload.state,
          officialBallotTitle: entry.official_ballot_title,
        });
        officeMatchCounts[officeMatch.method] += 1;
        matchedOfficeId = officeMatch.officeId;
        if (officeMatch.method === "none" || officeMatch.method === "ambiguous") {
          unresolvedOfficeMatches.push({
            method: officeMatch.method,
            confidence: officeMatch.confidence,
            officialBallotTitle: entry.official_ballot_title,
            normalizedAlias: officeMatch.normalizedAlias,
          });
        }

        if (
          officeMatch.officeId &&
          officeMatch.shouldPersistAlias &&
          officeMatch.aliasMemoryKey.length > 0
        ) {
          const aliasKey = `${payload.district_type}::${officeMatch.aliasMemoryKey}`;
          if (!seenAliasKeys.has(aliasKey)) {
            seenAliasKeys.add(aliasKey);
            aliasRowsToInsert.push({
              office_id: officeMatch.officeId,
              scope: payload.district_type,
              alias_text: entry.official_ballot_title,
              normalized_alias: officeMatch.aliasMemoryKey,
            });
          }
        }
      }

      const upsertResult = await client.query<{ id: string; race_type: string }>(
        `
          INSERT INTO public.elections (
            district_id,
            official_ballot_title,
            official_ballot_title_key,
            description,
            election_date,
            race_type,
            is_partisan,
            election_stage,
            sources,
            office_id
          ) VALUES ($1, $2, $3, $4, $5::date, $6, $7, $8, $9::jsonb, $10::uuid)
          ON CONFLICT (district_id, official_ballot_title_key, election_date) DO UPDATE SET
            description = EXCLUDED.description,
            race_type = EXCLUDED.race_type,
            -- Keep prior partisanship when a subsequent run omits it (e.g., mixed-state school contests).
            is_partisan = COALESCE(EXCLUDED.is_partisan, elections.is_partisan),
            election_stage = COALESCE(EXCLUDED.election_stage, elections.election_stage),
            office_id = COALESCE(EXCLUDED.office_id, elections.office_id),
            sources = EXCLUDED.sources,
            updated_at = now()
          RETURNING id, race_type
        `,
        [
          payload.district_id,
          entry.official_ballot_title,
          normalizeElectionTitleKey(entry.official_ballot_title),
          entry.description,
          entry.election_date,
          entry.race_type,
          entry.is_partisan ?? null,
          entry.election_stage ?? null,
          JSON.stringify(entry.sources),
          matchedOfficeId,
        ]
      );
      const row = upsertResult.rows?.[0];
      if (row?.race_type === "ballot_measure") {
        ballotMeasureElectionIds.push(row.id);
      } else if (row?.race_type === "office") {
        officeElectionIds.push(row.id);
        if (isUsSenateOfficeTitle(entry.official_ballot_title)) {
          senateMetadataRows.push({
            election_id: row.id,
            senate_class: entry.senate_class ?? null,
            term_end_year: entry.term_end_year ?? null,
          });
        }
      }
    }

    if (
      officeMatchCounts.alias_exact +
        officeMatchCounts.deterministic_fallback +
        officeMatchCounts.none +
        officeMatchCounts.ambiguous >
      0
    ) {
      console.log(
        `office-matcher summary ingest_key=${ingestKey} district_id=${payload.district_id} scope=${payload.district_type} ` +
          `alias_exact=${officeMatchCounts.alias_exact} fallback=${officeMatchCounts.deterministic_fallback} ` +
          `none=${officeMatchCounts.none} ambiguous=${officeMatchCounts.ambiguous}`
      );
    }

    for (const unresolved of unresolvedOfficeMatches) {
      console.log(
        `office-matcher unresolved ingest_key=${ingestKey} district_id=${payload.district_id} scope=${payload.district_type} ` +
          `method=${unresolved.method} confidence=${unresolved.confidence.toFixed(3)} ` +
          `title=${JSON.stringify(unresolved.officialBallotTitle)} normalized_alias=${JSON.stringify(unresolved.normalizedAlias)}`
      );
    }

    if (aliasRowsToInsert.length > 0) {
      await client.query(
        `
          INSERT INTO public.office_title_aliases (
            office_id,
            scope,
            alias_text,
            normalized_alias
          )
          SELECT
            a.office_id,
            a.scope,
            a.alias_text,
            a.normalized_alias
          FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[]) AS a(
            office_id,
            scope,
            alias_text,
            normalized_alias
          )
          ON CONFLICT (scope, normalized_alias) DO NOTHING
        `,
        [
          aliasRowsToInsert.map((row) => row.office_id),
          aliasRowsToInsert.map((row) => row.scope),
          aliasRowsToInsert.map((row) => row.alias_text),
          aliasRowsToInsert.map((row) => row.normalized_alias),
        ]
      );
    }

    if (senateMetadataRows.length > 0) {
      await client.query(
        `
          INSERT INTO public.election_senate_metadata (
            election_id,
            senate_class,
            term_end_year
          )
          SELECT
            m.election_id,
            m.senate_class,
            m.term_end_year
          FROM unnest($1::uuid[], $2::text[], $3::text[]) AS m(election_id, senate_class, term_end_year)
          ON CONFLICT (election_id) DO UPDATE
          SET senate_class = EXCLUDED.senate_class,
              term_end_year = EXCLUDED.term_end_year,
              updated_at = now()
        `,
        [
          senateMetadataRows.map((row) => row.election_id),
          senateMetadataRows.map((row) => row.senate_class),
          senateMetadataRows.map((row) => row.term_end_year),
        ]
      );
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

    if (officeElectionIds.length > 0) {
      await client.query(
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
          SELECT
            'candidate_roster:' || office_id::text AS ingest_key,
            $1::text AS item_type,
            jsonb_build_object('election_id', office_id::text) AS payload,
            'pending' AS status,
            NULL::text AS reason,
            $2::text AS run_id,
            NULL::text AS model,
            NULL::text AS schema_version,
            NULL::text AS prompt_version
          FROM unnest($3::uuid[]) AS office_ids(office_id)
          ON CONFLICT (ingest_key) DO NOTHING
        `,
        [STAGING_ITEM_TYPE_CANDIDATE_ROSTER, runId ?? "", officeElectionIds]
      );
    }

    await client.query("COMMIT");
    return {
      wrote: true,
      ballotMeasureElectionIds,
      officeElectionIds,
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
            let officeElectionIds: string[] = [];
            const client = await pool.connect();
            try {
              const writeResult = await writeElectionsForDistrict(
                client,
                ingestKey,
                parsed.payload,
                familySeedUrls,
                row.run_id
              );
              if (!writeResult.wrote) {
                const latestRow = await getStagingRow(pool, ingestKey);
                if (latestRow?.status === "written" || latestRow?.status === "no_results") {
                  const recoveredBallotMeasureIds = await resolveBallotMeasureElectionIds(pool, parsed.payload);
                  const recoveredOfficeIds = await resolveOfficeElectionIds(pool, parsed.payload);
                  const recoveredEligibleOfficeIds = await selectWriterEligibleOfficeElectionIds(
                    pool,
                    recoveredOfficeIds,
                    "recovery"
                  );
                  await enqueueBallotMeasureDrafts(redis, recoveredBallotMeasureIds, latestRow.run_id);
                  await enqueueCandidateRosterDrafts(redis, recoveredEligibleOfficeIds, latestRow.run_id);
                  await redis.xAdd(STAGING_WRITTEN_STREAM, "*", {
                    ingest_key: ingestKey,
                    item_type: STAGING_ITEM_TYPE_ELECTION,
                    run_id: latestRow.run_id ?? "",
                    payload: JSON.stringify(parsed.payload),
                  });
                }
                await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
                continue;
              }
              ballotMeasureElectionIds = writeResult.ballotMeasureElectionIds;
              officeElectionIds = writeResult.officeElectionIds;
            } finally {
              client.release();
            }
            const eligibleOfficeElectionIds = await selectWriterEligibleOfficeElectionIds(
              pool,
              officeElectionIds,
              "validated-write"
            );
            await enqueueBallotMeasureDrafts(redis, ballotMeasureElectionIds, row.run_id);
            await enqueueCandidateRosterDrafts(redis, eligibleOfficeElectionIds, row.run_id);
          } else if (row.status !== "written" && row.status !== "no_results") {
            await redis.xAck(STAGING_VALIDATED_STREAM, STAGING_ELECTIONS_WRITER_GROUP, entry.id);
            continue;
          } else {
            const ballotMeasureElectionIds = await resolveBallotMeasureElectionIds(pool, parsed.payload);
            const officeElectionIds = await resolveOfficeElectionIds(pool, parsed.payload);
            const eligibleOfficeElectionIds = await selectWriterEligibleOfficeElectionIds(
              pool,
              officeElectionIds,
              "replay-written"
            );
            await enqueueBallotMeasureDrafts(redis, ballotMeasureElectionIds, row.run_id);
            await enqueueCandidateRosterDrafts(redis, eligibleOfficeElectionIds, row.run_id);
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
