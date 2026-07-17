import { Pool } from "pg";
import { createClient } from "redis";

import {
  buildCandidateRosterConfigFromEnv,
  type CandidateDuplicateDisambiguationInput,
  type CandidateDuplicateDisambiguationResult,
  disambiguateCandidateDuplicateGroup,
  enrichCandidateRoster,
} from "../../ai/enrichCandidateRoster.js";
import { resolveCandidateResearchMode } from "../../ai/candidateResearchMode.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_CANDIDATE_ROSTER_REJECTED_STREAM,
  STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
  STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
  STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
} from "../../config/electionsPipeline.js";
import { normalizeCandidateName, splitDisplayNameToFirstLast } from "../../utils/candidateIdentity.js";
import { parseCandidateRosterPayload, type CandidateRosterEntry } from "../../contracts/candidateRosterPayloadContract.js";
import {
  defaultOfficeCandidateEligibilityConfig,
  getOfficeCandidateEligibilityForElectionId,
} from "../candidates/officeCandidateEligibility.js";
import { enqueueCandidateProfileDrafts } from "../candidates/candidateProfileDraftEmitter.js";

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
  election_stage: string | null;
  senate_class: string | null;
  term_end_year: string | null;
  is_partisan: boolean | null;
  sources: unknown;
};

type CandidateRosterStagingRow = {
  ingest_key: string;
  payload: unknown;
  status: string;
  run_id: string | null;
};

export type CandidateRosterResolvedEntry = CandidateRosterEntry & {
  roster_index: number;
  disambiguation_hint?: string;
  skip_per_election_name_dedupe?: boolean;
};
type CandidateRosterIndexedEntry = CandidateRosterEntry & {
  roster_index: number;
};

type DisambiguateDuplicateGroupFn = (
  input: CandidateDuplicateDisambiguationInput,
  config: ReturnType<typeof buildCandidateRosterConfigFromEnv>
) => Promise<CandidateDuplicateDisambiguationResult>;

const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const MAX_SEED_URLS = 8;
const MAX_DELIVERY_ATTEMPTS = 8;
const CANDIDATE_ROSTER_STAGING_PREFIX = "candidate_roster:";
const ENABLE_CANDIDATE_ROSTER_ENRICHER_ELIGIBILITY_GATE =
  process.env.CANDIDATE_ROSTER_ENABLE_ENRICHER_ELIGIBILITY_GATE === "true";

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

function normalizeDisplayNameFirstLast(displayName: string): string {
  const name = splitDisplayNameToFirstLast(displayName);
  return normalizeCandidateName(`${name.firstName} ${name.lastName}`);
}

function normalizeFecIds(values: readonly string[] | undefined): string[] {
  return [...new Set((values ?? []).map((value) => value.trim().toUpperCase()).filter((value) => value.length > 0))].sort();
}

function normalizeStateFilingIds(values: readonly string[] | undefined): string[] {
  return [...new Set((values ?? []).map((value) => value.trim().toUpperCase()).filter((value) => value.length > 0))].sort();
}

function rosterIngestKeyForElection(electionId: string): string {
  return `${CANDIDATE_ROSTER_STAGING_PREFIX}${electionId}`;
}

function extractRosterCandidatesFromStagingPayload(payload: unknown): CandidateRosterResolvedEntry[] | null {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return null;
  }
  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.candidates)) {
    return null;
  }

  const resolved: CandidateRosterResolvedEntry[] = [];
  for (const [rowIndex, row] of input.candidates.entries()) {
    const parsedRow = parseCandidateRosterPayload({ candidates: [row] });
    if (!parsedRow.ok || parsedRow.payload.candidates.length !== 1) {
      return null;
    }
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      return null;
    }
    const raw = row as Record<string, unknown>;
    const rosterIndex =
      Number.isInteger(raw.roster_index) && Number(raw.roster_index) >= 0 ? Number(raw.roster_index) : rowIndex;

    const disambiguationHint =
      typeof raw.disambiguation_hint === "string" && raw.disambiguation_hint.trim().length > 0
        ? raw.disambiguation_hint.trim()
        : undefined;
    const skipNameDedupe =
      raw.skip_per_election_name_dedupe === true ? true : raw.skip_per_election_name_dedupe === false ? false : undefined;
    const fecIds =
      Array.isArray(raw.fec_ids) && raw.fec_ids.every((item) => typeof item === "string")
        ? [...new Set(raw.fec_ids.map((item) => item.trim()).filter((item) => item.length > 0))]
        : undefined;

    resolved.push({
      ...parsedRow.payload.candidates[0]!,
      roster_index: rosterIndex,
      ...(disambiguationHint ? { disambiguation_hint: disambiguationHint } : {}),
      ...(fecIds && fecIds.length > 0 ? { fec_ids: fecIds } : {}),
      ...(skipNameDedupe !== undefined ? { skip_per_election_name_dedupe: skipNameDedupe } : {}),
    });
  }
  return resolved;
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
      VALUES ($1, $2, jsonb_build_object('election_id', $3::text), 'pending', NULL, $4, NULL, NULL, NULL)
      ON CONFLICT (ingest_key) DO NOTHING
    `,
    [ingestKey, STAGING_ITEM_TYPE_CANDIDATE_ROSTER, electionId, runId ?? ""]
  );
}

async function markCandidateRosterStagingValidated(
  pool: Pool,
  ingestKey: string,
  electionId: string,
  candidates: CandidateRosterResolvedEntry[],
  runId: string | null,
  aiRawDebug: Record<string, unknown> | null
): Promise<void> {
  await pool.query(
    `
      UPDATE staging_items
      SET payload = jsonb_build_object('election_id', $2::text, 'candidates', $3::jsonb),
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

// An AI roster run that found zero candidates is a "nothing announced yet"
// outcome, not a completed roster: stamping it 'written' would make the
// election indistinguishable from one with a real roster. 'no_results'
// (same semantics as the elections writer) keeps that distinction —
// manual:candidate-roster:due surfaces it for a manual refresh. The
// automated eligibility gate still treats it as complete, so the rollover
// producer never auto-retries it.
async function markCandidateRosterStagingNoResults(
  pool: Pool,
  ingestKey: string,
  electionId: string,
  runId: string | null,
  aiRawDebug: Record<string, unknown> | null
): Promise<void> {
  await pool.query(
    `
      UPDATE staging_items
      SET payload = jsonb_build_object('election_id', $2::text, 'candidates', '[]'::jsonb),
          status = 'no_results',
          reason = NULL,
          failure_debug = NULL,
          ai_raw_debug = $3::jsonb,
          run_id = $4,
          written_at = now(),
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $5
    `,
    [
      ingestKey,
      electionId,
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

export async function filterAlreadyLinkedCandidates(
  pool: Pool,
  electionId: string,
  candidates: CandidateRosterIndexedEntry[]
): Promise<CandidateRosterIndexedEntry[]> {
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

  const inputNameCounts = new Map<string, number>();
  for (const candidate of candidates) {
    const normalizedName = normalizeDisplayNameFirstLast(candidate.display_name);
    if (normalizedName.length === 0) {
      continue;
    }
    inputNameCounts.set(normalizedName, (inputNameCounts.get(normalizedName) ?? 0) + 1);
  }

  return candidates.filter((candidate) => {
    const normalizedName = normalizeDisplayNameFirstLast(candidate.display_name);
    if (normalizedName.length === 0) {
      return false;
    }
    if ((inputNameCounts.get(normalizedName) ?? 0) > 1) {
      return true;
    }
    if (candidate.running_mate) {
      return true;
    }
    return !linkedNames.has(normalizedName);
  });
}

export async function resolveCandidateRosterForProfileDrafts(
  input: {
    districtName: string;
    districtType: string;
    state: string;
    electionDate: string;
    officialBallotTitle: string;
    electionStage?: string | null;
    senateClass?: string | null;
    termEndYear?: string | null;
    electionIsPartisan?: boolean | null;
    seedUrls: readonly string[];
    candidates: CandidateRosterIndexedEntry[];
  },
  aiConfig: ReturnType<typeof buildCandidateRosterConfigFromEnv>,
  disambiguateDuplicateGroup: DisambiguateDuplicateGroupFn = disambiguateCandidateDuplicateGroup
): Promise<{
  resolvedCandidates: CandidateRosterResolvedEntry[];
  debug: Record<string, unknown>;
}> {
  const researchMode = resolveCandidateResearchMode({
    districtType: input.districtType,
    officialBallotTitle: input.officialBallotTitle,
  });
  const isFederalMode = researchMode !== "state_level";
  const resolvedCandidates: CandidateRosterResolvedEntry[] = [];
  const grouped = new Map<string, CandidateRosterResolvedEntry[]>();
  for (const candidate of input.candidates) {
    const key = normalizeCandidateName(candidate.display_name);
    if (key.length === 0) {
      resolvedCandidates.push(candidate);
      continue;
    }
    const existing = grouped.get(key);
    if (existing) {
      existing.push(candidate);
    } else {
      grouped.set(key, [candidate]);
    }
  }

  const duplicateGroupsDebug: Array<Record<string, unknown>> = [];

  for (const group of grouped.values()) {
    if (group.length <= 1) {
      resolvedCandidates.push(group[0]!);
      continue;
    }

    const normalizedPartySet = new Set(
      group
        .map((candidate) => candidate.party?.trim().toLowerCase() ?? "")
        .filter((party) => party.length > 0)
    );

    if (normalizedPartySet.size > 1) {
      for (const candidate of group) {
        resolvedCandidates.push({
          ...candidate,
          skip_per_election_name_dedupe: true,
        });
      }
      duplicateGroupsDebug.push({
        duplicate_display_name: group[0]!.display_name,
        strategy: "party_diff_fast_path",
        group_size: group.length,
        selected_count: group.length,
      });
      continue;
    }

    if (isFederalMode) {
      const byFecKey = new Map<string, CandidateRosterResolvedEntry>();
      for (const candidate of group) {
        const fecIds = normalizeFecIds(candidate.fec_ids);
        const fecKey = fecIds.join("|");
        const existing = byFecKey.get(fecKey);
        if (!existing) {
          byFecKey.set(fecKey, {
            ...candidate,
            skip_per_election_name_dedupe: true,
            fec_ids: fecIds,
          });
          continue;
        }

        const mergedSources = [...new Set([...existing.sources, ...candidate.sources])];
        byFecKey.set(fecKey, {
          ...existing,
          sources: mergedSources,
          ...(existing.party ? {} : candidate.party ? { party: candidate.party } : {}),
          ...(existing.is_incumbent !== undefined
            ? {}
            : candidate.is_incumbent !== undefined
              ? { is_incumbent: candidate.is_incumbent }
              : {}),
        });
      }

      for (const candidate of byFecKey.values()) {
        resolvedCandidates.push({
          ...candidate,
          skip_per_election_name_dedupe: true,
        });
      }
      duplicateGroupsDebug.push({
        duplicate_display_name: group[0]!.display_name,
        strategy: "federal_fec_strict_no_ai_disambiguation",
        group_size: group.length,
        selected_count: byFecKey.size,
        dropped_same_fec_count: Math.max(0, group.length - byFecKey.size),
        research_mode: researchMode,
      });
      continue;
    }

    const allHaveStateFilingIds = group.every(
      (candidate) => normalizeStateFilingIds(candidate.state_filing_ids).length > 0
    );
    if (allHaveStateFilingIds) {
      const byStateFilingKey = new Map<string, CandidateRosterResolvedEntry>();
      for (const candidate of group) {
        const filingIds = normalizeStateFilingIds(candidate.state_filing_ids);
        const filingKey = filingIds.join("|");
        const existing = byStateFilingKey.get(filingKey);
        if (!existing) {
          byStateFilingKey.set(filingKey, {
            ...candidate,
            state_filing_ids: filingIds,
            skip_per_election_name_dedupe: true,
          });
          continue;
        }

        const mergedSources = [...new Set([...existing.sources, ...candidate.sources])];
        byStateFilingKey.set(filingKey, {
          ...existing,
          sources: mergedSources,
          ...(existing.party ? {} : candidate.party ? { party: candidate.party } : {}),
          ...(existing.is_incumbent !== undefined
            ? {}
            : candidate.is_incumbent !== undefined
              ? { is_incumbent: candidate.is_incumbent }
              : {}),
        });
      }

      for (const candidate of byStateFilingKey.values()) {
        resolvedCandidates.push({
          ...candidate,
          skip_per_election_name_dedupe: true,
        });
      }
      duplicateGroupsDebug.push({
        duplicate_display_name: group[0]!.display_name,
        strategy: "state_filing_ids_strict_no_ai_disambiguation",
        group_size: group.length,
        selected_count: byStateFilingKey.size,
        dropped_same_state_filing_ids_count: Math.max(0, group.length - byStateFilingKey.size),
      });
      continue;
    }

    const disambiguationResult = await disambiguateDuplicateGroup(
      {
        districtName: input.districtName,
        districtType: input.districtType,
        state: input.state,
        electionDate: input.electionDate,
        officialBallotTitle: input.officialBallotTitle,
        electionStage: input.electionStage,
        senateClass: input.senateClass,
        termEndYear: input.termEndYear,
        electionIsPartisan: input.electionIsPartisan,
        duplicateDisplayName: group[0]!.display_name,
        options: group.map((candidate) => ({
          roster_index: candidate.roster_index,
          display_name: candidate.display_name,
          party: candidate.party,
          is_incumbent: candidate.is_incumbent,
          sources: candidate.sources,
        })),
        seedUrls: input.seedUrls,
      },
      aiConfig
    );

    if (!disambiguationResult.ok) {
      duplicateGroupsDebug.push({
        duplicate_display_name: group[0]!.display_name,
        strategy: "ai_failure_fallback_keep_one",
        group_size: group.length,
        reason: disambiguationResult.reason,
      });
      resolvedCandidates.push(group[0]!);
      continue;
    }

    const clearByIndex = new Map<number, { hint: string; fecIds: string[] | undefined }>();
    const ambiguousIndexes: number[] = [];
    const mergedByTarget = new Map<number, number[]>();
    for (const person of disambiguationResult.people) {
      if (person.status === "clear" && person.disambiguation_hint) {
        if (!clearByIndex.has(person.roster_index)) {
          clearByIndex.set(person.roster_index, {
            hint: person.disambiguation_hint,
            fecIds: person.fec_ids,
          });
        }
      } else if (person.status === "ambiguous") {
        ambiguousIndexes.push(person.roster_index);
      } else if (person.status === "same_as_other" && person.same_as_roster_index !== undefined) {
        const merged = mergedByTarget.get(person.same_as_roster_index) ?? [];
        if (!merged.includes(person.roster_index)) {
          merged.push(person.roster_index);
        }
        mergedByTarget.set(person.same_as_roster_index, merged);
      }
    }

    for (const candidate of group) {
      const clear = clearByIndex.get(candidate.roster_index);
      if (!clear) {
        continue;
      }
      resolvedCandidates.push({
        ...candidate,
        skip_per_election_name_dedupe: true,
        disambiguation_hint: clear.hint,
        ...(clear.fecIds && clear.fecIds.length > 0 ? { fec_ids: clear.fecIds } : {}),
      });
    }

    if (clearByIndex.size === 0) {
      // Conservative fallback when AI cannot clear any row in this duplicate-name group.
      resolvedCandidates.push(group[0]!);
      duplicateGroupsDebug.push({
        duplicate_display_name: group[0]!.display_name,
        strategy: "ai_all_ambiguous_keep_one",
        group_size: group.length,
        selected_roster_index: group[0]!.roster_index,
        dropped_ambiguous_roster_indexes: ambiguousIndexes,
      });
      continue;
    }

    duplicateGroupsDebug.push({
      duplicate_display_name: group[0]!.display_name,
      strategy: "ai_person_level_partial_keep",
      group_size: group.length,
      selected_count: clearByIndex.size,
      kept_roster_indexes: [...clearByIndex.keys()],
      dropped_ambiguous_roster_indexes: ambiguousIndexes,
      merged_roster_indexes: Object.fromEntries(
        [...mergedByTarget.entries()].map(([targetIndex, mergedIndexes]) => [
          `merged_into_${targetIndex}`,
          mergedIndexes,
        ])
      ),
    });
  }

  return {
    resolvedCandidates,
    debug: {
      duplicate_groups: duplicateGroupsDebug,
    },
  };
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
        e.election_stage::text AS election_stage,
        sm.senate_class,
        sm.term_end_year,
        e.is_partisan,
        e.sources
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      LEFT JOIN public.election_senate_metadata AS sm
        ON sm.election_id = e.id
      WHERE e.id = $1
        AND e.race_type = 'office'
      LIMIT 1
    `,
    [electionId]
  );

  return result.rows[0] ?? null;
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

          if (ENABLE_CANDIDATE_ROSTER_ENRICHER_ELIGIBILITY_GATE) {
            const eligibility = await getOfficeCandidateEligibilityForElectionId(
              pool,
              electionId,
              defaultOfficeCandidateEligibilityConfig()
            );
            if (eligibility.reason !== "eligible") {
              console.log(
                `candidate-roster enricher eligibility-gate skip election_id=${electionId} reason=${eligibility.reason}`
              );
              await redis.xAck(
                STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
                STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
                entry.id
              );
              continue;
            }
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

          let candidatesForFanout: CandidateRosterResolvedEntry[] | null = null;
          if (stagingRow.status === "validated" || stagingRow.status === "written") {
            candidatesForFanout = extractRosterCandidatesFromStagingPayload(stagingRow.payload);
            if (!candidatesForFanout) {
              throw new Error(`invalid candidate roster staging payload for ${ingestKey}`);
            }
          } else {
            const aiResult = await enrichCandidateRoster(
              {
                districtName: election.district_name,
                districtType: election.district_type,
                state: election.state,
                electionDate: election.election_date,
                officialBallotTitle: election.official_ballot_title,
                electionStage: election.election_stage,
                senateClass: election.senate_class,
                termEndYear: election.term_end_year,
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

            // Checked before the already-linked filter: an empty post-filter
            // list can also mean every candidate is already linked (roster
            // complete), so only the raw AI result signals "none found".
            if (aiResult.candidates.length === 0) {
              await markCandidateRosterStagingNoResults(
                pool,
                ingestKey,
                electionId,
                runId || stagingRow.run_id,
                aiResult.aiRawDebug ?? null
              );
              console.log(`candidate-roster enricher no results election_id=${electionId}`);
              await redis.xAck(
                STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
                STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP,
                entry.id
              );
              continue;
            }

            const indexedCandidates: CandidateRosterIndexedEntry[] = aiResult.candidates.map((candidate, rosterIndex) => ({
              ...candidate,
              roster_index: rosterIndex,
            }));
            const filteredCandidates = await filterAlreadyLinkedCandidates(pool, electionId, indexedCandidates);
            const resolved = await resolveCandidateRosterForProfileDrafts(
              {
                districtName: election.district_name,
                districtType: election.district_type,
                state: election.state,
                electionDate: election.election_date,
                officialBallotTitle: election.official_ballot_title,
                electionStage: election.election_stage,
                senateClass: election.senate_class,
                termEndYear: election.term_end_year,
                electionIsPartisan: election.is_partisan,
                seedUrls: parseSeedUrls(election.sources),
                candidates: filteredCandidates,
              },
              aiConfig
            );
            const mergedRosterDebug = {
              ...(aiResult.aiRawDebug ?? {}),
              duplicate_resolution: resolved.debug,
            };
            await markCandidateRosterStagingValidated(
              pool,
              ingestKey,
              electionId,
              resolved.resolvedCandidates,
              runId || stagingRow.run_id,
              mergedRosterDebug
            );
            candidatesForFanout = resolved.resolvedCandidates;
          }

          const electionSeedUrls = parseSeedUrls(election.sources);
          for (const candidate of candidatesForFanout) {
            await enqueueCandidateProfileDrafts(redis, [
              {
                electionId,
                runId,
                displayName: candidate.display_name,
                rosterIndex: candidate.roster_index,
                rosterParty: candidate.party,
                rosterIsIncumbent: candidate.is_incumbent,
                disambiguationHint: candidate.disambiguation_hint,
                fecIds: candidate.fec_ids,
                stateFilingIdsHint: candidate.state_filing_ids,
                skipPerElectionNameDedupe: candidate.skip_per_election_name_dedupe,
                seedUrls: mergeSeedUrls(candidate.sources, electionSeedUrls),
              },
              ...(candidate.running_mate
                ? [
                    {
                      electionId,
                      runId,
                      displayName: candidate.running_mate.display_name,
                      rosterIndex: candidate.roster_index,
                      rosterParty: candidate.running_mate.party,
                      seedUrls: mergeSeedUrls(candidate.running_mate.sources, electionSeedUrls),
                      electionTicketRole: "running_mate" as const,
                      ticketLeadDisplayName: candidate.display_name,
                    },
                  ]
                : []),
            ]);
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
