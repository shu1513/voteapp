import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";

import { assertKnownCliFlags } from "./manualCliFlags.js";
import {
  listMissingSweepRouteQuestionIds,
  SWEEP_ROUTE_QUESTION_IDS,
  type SweepRoute,
} from "./candidateRecordSweepEvidence.js";
/**
 * Red-flag audit for false records-sweep completeness.
 *
 * A candidate whose profile shows real service (a current office or an
 * incumbent election link) but who has ZERO candidate_records rows despite a
 * `last_records_searched_at` completion stamp is the signature of a skipped
 * discovery sweep written as `no_records_found`. This read-only script lists
 * those candidates so a session can re-run their record sweeps properly.
 *
 * Candidates whose zero-record state is backed by a persisted sweep
 * confirmation (candidate_record_sweep_confirmations, written by the
 * evidence-file guard) are reported separately as confirmed nulls, not
 * suspects — their sweep was finished and evidenced, so they need no re-run.
 *
 * The confirmation only counts while it covers the latest completion stamp:
 * writer and confirmation share one transaction (both timestamps are that
 * transaction's now()), so a fresh confirmation compares equal — but any
 * LATER search that re-stamps last_records_searched_at without refreshing
 * the evidence row (AI worker pass, pre-guard manual write) makes the old
 * evidence a historical claim, and the candidate goes back to suspects.
 *
 * Usage: npm run manual:records:audit [-- --candidate-id uuid] [--election-id uuid] [--district-id uuid]
 *
 * Targeting flags narrow the audit to one candidate, the candidates linked to
 * one election, or the candidates linked to any election in one district —
 * useful right after a per-district or per-election research pass instead of
 * scanning the whole table. Flags combine with AND.
 */

type AuditRow = {
  candidate_id: string;
  display_name: string;
  current_office: string | null;
  is_incumbent: boolean;
  last_records_searched_at: string;
  election_titles: string[];
  confirmed_gap_ids: string[] | null;
  confirmed_at: string | null;
  evidence_entry_count: number | null;
  confirmation_covers_latest_search: boolean | null;
};

const NO_RECORDS_FOUND_GAP_ID = "candidate_records.no_records_found";

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export type AuditTargetFilters = {
  candidateId: string | null;
  electionId: string | null;
  districtId: string | null;
};

export function buildAuditTargetConditions(filters: AuditTargetFilters): {
  conditions: string[];
  values: string[];
} {
  const conditions: string[] = [];
  const values: string[] = [];
  const push = (value: string, condition: (placeholder: string) => string): void => {
    values.push(value);
    conditions.push(condition(`$${values.length}`));
  };
  if (filters.candidateId) {
    push(filters.candidateId, (p) => `c.id = ${p}::uuid`);
  }
  // Joint-ticket running mates have no candidate_elections row of their own
  // (they live in running_mate_candidate_id on the ticket lead's row), so the
  // election/district predicates must accept either side of the link.
  if (filters.electionId) {
    push(
      filters.electionId,
      (p) =>
        `EXISTS (SELECT 1 FROM public.candidate_elections cef WHERE (cef.candidate_id = c.id OR cef.running_mate_candidate_id = c.id) AND cef.election_id = ${p}::uuid)`
    );
  }
  if (filters.districtId) {
    push(
      filters.districtId,
      (p) =>
        `EXISTS (SELECT 1 FROM public.candidate_elections cef JOIN public.elections ef ON ef.id = cef.election_id WHERE (cef.candidate_id = c.id OR cef.running_mate_candidate_id = c.id) AND ef.district_id = ${p}::uuid)`
    );
  }
  return { conditions, values };
}

function readUuidFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index < 0) {
    return null;
  }
  const value = process.argv[index + 1]?.trim();
  if (!value || value.startsWith("--")) {
    throw new Error(`Missing value for ${name}`);
  }
  if (!UUID_PATTERN.test(value)) {
    throw new Error(`${name} must be a UUID, got "${value}"`);
  }
  return value;
}

export function isConfirmedNull(
  row: Pick<AuditRow, "confirmed_gap_ids" | "confirmation_covers_latest_search">
): boolean {
  return (
    row.confirmation_covers_latest_search === true &&
    (row.confirmed_gap_ids ?? []).includes(NO_RECORDS_FOUND_GAP_ID)
  );
}

// --- Sweep-confirmation red-flag detectors (PR 2 of the routing-enforcement
// plan). Both read the persisted evidence ledgers and retro-flag the
// 2026-07-15 collapsed-template cohort: identical multi-candidate finding
// text is the copy-paste signature, and a ledger whose question_id tags
// cover no complete route was never a per-question sweep.

/**
 * Identical finding text across at least this many DISTINCT candidates flags
 * the group. Real research phrases findings per candidate; verbatim reuse
 * across several people is the template signature.
 */
export const SHARED_FINDING_MIN_CANDIDATES = 3;

/**
 * Findings shorter than this never flag: short negative findings ("nothing
 * found" is the sanctioned phrasing for an empty answer) legitimately repeat
 * across candidates, while identical long prose does not.
 */
export const SHARED_FINDING_MIN_LENGTH = 40;

export type SweepConfirmationDetectorRow = {
  candidate_id: string;
  display_name: string;
  has_held_public_office: boolean | null;
  confirmed_at: string;
  evidence: unknown;
};

type EvidenceEntryForAudit = {
  finding: string;
  questionId: string | null;
};

/**
 * Defensive read of the stored evidence jsonb ({"entries": [{"question",
 * "finding", "question_id"?}]}); malformed rows yield no entries rather than
 * crashing the audit — the route-coverage detector then flags them anyway
 * (no tags = no covered route).
 */
export function extractAuditEvidenceEntries(evidence: unknown): EvidenceEntryForAudit[] {
  if (typeof evidence !== "object" || evidence === null || Array.isArray(evidence)) {
    return [];
  }
  const entries = (evidence as Record<string, unknown>).entries;
  if (!Array.isArray(entries)) {
    return [];
  }
  const parsed: EvidenceEntryForAudit[] = [];
  for (const row of entries) {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      continue;
    }
    const entry = row as Record<string, unknown>;
    if (typeof entry.finding !== "string" || entry.finding.trim().length === 0) {
      continue;
    }
    parsed.push({
      finding: entry.finding.trim(),
      questionId: typeof entry.question_id === "string" ? entry.question_id : null,
    });
  }
  return parsed;
}

export type SharedFindingTextGroup = {
  findingText: string;
  candidateCount: number;
  sampleCandidates: { candidateId: string; displayName: string }[];
};

/**
 * Groups confirmations by normalized finding text and returns the groups
 * shared verbatim across >= SHARED_FINDING_MIN_CANDIDATES distinct
 * candidates, most-shared first.
 */
export function listSharedFindingTexts(
  rows: readonly SweepConfirmationDetectorRow[]
): SharedFindingTextGroup[] {
  const groups = new Map<
    string,
    { findingText: string; candidates: Map<string, string> }
  >();
  for (const row of rows) {
    for (const entry of extractAuditEvidenceEntries(row.evidence)) {
      if (entry.finding.length < SHARED_FINDING_MIN_LENGTH) {
        continue;
      }
      const normalized = entry.finding.toLowerCase().replace(/\s+/g, " ");
      let group = groups.get(normalized);
      if (!group) {
        group = { findingText: entry.finding, candidates: new Map() };
        groups.set(normalized, group);
      }
      if (!group.candidates.has(row.candidate_id)) {
        group.candidates.set(row.candidate_id, row.display_name);
      }
    }
  }
  return [...groups.values()]
    .filter((group) => group.candidates.size >= SHARED_FINDING_MIN_CANDIDATES)
    .sort((left, right) => right.candidates.size - left.candidates.size)
    .map((group) => ({
      findingText: group.findingText,
      candidateCount: group.candidates.size,
      sampleCandidates: [...group.candidates.entries()]
        .slice(0, 5)
        .map(([candidateId, displayName]) => ({ candidateId, displayName })),
    }));
}

export type RouteCoverageGap = {
  candidateId: string;
  displayName: string;
  confirmedAt: string;
  taggedQuestionIds: string[];
  reason: string;
};

/**
 * A confirmation must carry question_id tags covering at least one COMPLETE
 * route question list consistent with the candidate's stored routing fact:
 * has_held_public_office=true allows officeholder or judicial, false allows
 * never_held_office or judicial (judicial contests are routed by contest
 * family, which both officeholders and first-timers can sit in), NULL allows
 * any route. Pre-PR-#350 confirmations carry no tags and flag here — that is
 * the retro-flagging of the 2026-07-15 cohort, not a false positive.
 */
export function listRouteCoverageGaps(
  rows: readonly SweepConfirmationDetectorRow[]
): RouteCoverageGap[] {
  const gaps: RouteCoverageGap[] = [];
  for (const row of rows) {
    const entries = extractAuditEvidenceEntries(row.evidence).map((entry) => ({
      question: "",
      finding: entry.finding,
      questionId: entry.questionId,
    }));
    const allowedRoutes: SweepRoute[] =
      row.has_held_public_office === true
        ? ["officeholder", "judicial"]
        : row.has_held_public_office === false
          ? ["never_held_office", "judicial"]
          : (Object.keys(SWEEP_ROUTE_QUESTION_IDS) as SweepRoute[]);
    const covered = allowedRoutes.some(
      (route) => listMissingSweepRouteQuestionIds(entries, route).length === 0
    );
    if (covered) {
      continue;
    }
    const taggedQuestionIds = [
      ...new Set(entries.map((entry) => entry.questionId).filter((id): id is string => id !== null)),
    ].sort();
    gaps.push({
      candidateId: row.candidate_id,
      displayName: row.display_name,
      confirmedAt: row.confirmed_at,
      taggedQuestionIds,
      reason:
        taggedQuestionIds.length === 0
          ? "no question_id tags (pre-#350 ledger or untagged template)"
          : `tags cover no complete route question list (allowed: ${allowedRoutes.join(", ")})`,
    });
  }
  return gaps;
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:records:audit", process.argv.slice(2), [
    { name: "--candidate-id", value: "space" },
    { name: "--election-id", value: "space" },
    { name: "--district-id", value: "space" },
  ]);
  loadProjectEnv();

  const filters: AuditTargetFilters = {
    candidateId: readUuidFlag("--candidate-id"),
    electionId: readUuidFlag("--election-id"),
    districtId: readUuidFlag("--district-id"),
  };
  const target = buildAuditTargetConditions(filters);
  const targetSql = target.conditions.map((condition) => `          AND ${condition}`).join("\n");

  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for manual records audit");
  }
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const result = await pool.query<AuditRow>(
      `
        SELECT
          c.id::text AS candidate_id,
          c.display_name,
          c.current_office,
          coalesce(bool_or(ce.is_incumbent), false) AS is_incumbent,
          c.last_records_searched_at::text AS last_records_searched_at,
          array_remove(array_agg(DISTINCT e.official_ballot_title), NULL) AS election_titles,
          sc.confirmed_gap_ids,
          sc.confirmed_at::text AS confirmed_at,
          jsonb_array_length(sc.evidence -> 'entries')::int AS evidence_entry_count,
          (sc.confirmed_at >= c.last_records_searched_at) AS confirmation_covers_latest_search
        FROM public.candidates c
        LEFT JOIN public.candidate_elections ce
          ON ce.candidate_id = c.id OR ce.running_mate_candidate_id = c.id
        LEFT JOIN public.elections e ON e.id = ce.election_id
        LEFT JOIN public.candidate_record_sweep_confirmations sc ON sc.candidate_id = c.id
        WHERE c.deleted_at IS NULL
          AND c.merged_into_candidate_id IS NULL
          AND c.last_records_searched_at IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM public.candidate_records r WHERE r.candidate_id = c.id
          )
${targetSql}
        GROUP BY c.id, c.display_name, c.current_office, c.last_records_searched_at,
          sc.confirmed_gap_ids, sc.confirmed_at, sc.evidence
        HAVING (c.current_office IS NOT NULL OR coalesce(bool_or(ce.is_incumbent), false))
        ORDER BY c.display_name ASC
      `,
      target.values
    );

    const suspects = result.rows.filter((row) => !isConfirmedNull(row));
    const confirmedNulls = result.rows.filter((row) => isConfirmedNull(row));

    // Detector pass over ALL persisted sweep confirmations in scope (not just
    // zero-record candidates): a collapsed-template ledger is a red flag even
    // when the candidate has records.
    const confirmationsResult = await pool.query<SweepConfirmationDetectorRow>(
      `
        SELECT
          c.id::text AS candidate_id,
          c.display_name,
          c.has_held_public_office,
          sc.confirmed_at::text AS confirmed_at,
          sc.evidence
        FROM public.candidate_record_sweep_confirmations sc
        JOIN public.candidates c ON c.id = sc.candidate_id
        WHERE c.deleted_at IS NULL
          AND c.merged_into_candidate_id IS NULL
${targetSql}
        ORDER BY c.display_name ASC
      `,
      target.values
    );
    const sharedFindingTexts = listSharedFindingTexts(confirmationsResult.rows);
    const routeCoverageGaps = listRouteCoverageGaps(confirmationsResult.rows);
    const ROUTE_COVERAGE_GAP_LIST_LIMIT = 100;
    const SHARED_FINDING_LIST_LIMIT = 50;

    const appliedFilters = Object.fromEntries(
      Object.entries(filters).filter(([, value]) => value !== null)
    );

    console.log(
      JSON.stringify(
        {
          ...(Object.keys(appliedFilters).length > 0 ? { filters: appliedFilters } : {}),
          suspectCount: suspects.length,
          confirmedNullCount: confirmedNulls.length,
          explanation:
            "Suspects: candidates with a records-search completion stamp, a current office or incumbent election link, ZERO candidate_records rows, and no sweep confirmation covering the latest search (a confirmation older than last_records_searched_at is historical — a later search re-opened the question). Each needs a proper per-question record sweep re-run. Confirmed nulls carry an evidence-backed candidate_record_sweep_confirmations row at least as new as the latest completion stamp and need no re-run.",
          suspects: suspects.map((row) => ({
            candidateId: row.candidate_id,
            displayName: row.display_name,
            currentOffice: row.current_office,
            isIncumbent: row.is_incumbent,
            lastRecordsSearchedAt: row.last_records_searched_at,
            electionTitles: row.election_titles,
            ...(row.confirmed_at
              ? {
                  staleConfirmation: {
                    confirmedGapIds: row.confirmed_gap_ids,
                    confirmedAt: row.confirmed_at,
                    evidenceEntryCount: row.evidence_entry_count,
                  },
                }
              : {}),
          })),
          confirmedNulls: confirmedNulls.map((row) => ({
            candidateId: row.candidate_id,
            displayName: row.display_name,
            currentOffice: row.current_office,
            isIncumbent: row.is_incumbent,
            lastRecordsSearchedAt: row.last_records_searched_at,
            electionTitles: row.election_titles,
            confirmedGapIds: row.confirmed_gap_ids,
            confirmedAt: row.confirmed_at,
            evidenceEntryCount: row.evidence_entry_count,
          })),
          sweepConfirmationDetectors: {
            confirmationCount: confirmationsResult.rows.length,
            explanation:
              `Red-flag detectors over persisted sweep-confirmation ledgers. sharedFindingTexts: finding text (>= ${SHARED_FINDING_MIN_LENGTH} chars) repeated verbatim across >= ${SHARED_FINDING_MIN_CANDIDATES} distinct candidates — the copy-paste template signature; short negative findings like "nothing found" legitimately repeat and are exempt. routeCoverageGaps: confirmations whose question_id tags cover no complete route question list consistent with candidates.has_held_public_office — untagged pre-#350 ledgers (including the 2026-07-15 cohort) flag here by design. Flagged candidates need a proper per-question re-sweep (or the PR-3 confirmation-reset repair).`,
            sharedFindingTextCount: sharedFindingTexts.length,
            sharedFindingTexts: sharedFindingTexts.slice(0, SHARED_FINDING_LIST_LIMIT).map((group) => ({
              candidateCount: group.candidateCount,
              findingText:
                group.findingText.length > 200
                  ? `${group.findingText.slice(0, 197)}...`
                  : group.findingText,
              sampleCandidates: group.sampleCandidates,
            })),
            ...(sharedFindingTexts.length > SHARED_FINDING_LIST_LIMIT
              ? { sharedFindingTextsTruncatedTo: SHARED_FINDING_LIST_LIMIT }
              : {}),
            routeCoverageGapCount: routeCoverageGaps.length,
            routeCoverageGaps: routeCoverageGaps.slice(0, ROUTE_COVERAGE_GAP_LIST_LIMIT),
            ...(routeCoverageGaps.length > ROUTE_COVERAGE_GAP_LIST_LIMIT
              ? { routeCoverageGapsTruncatedTo: ROUTE_COVERAGE_GAP_LIST_LIMIT }
              : {}),
          },
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual records audit failed:", message);
    process.exitCode = 1;
  });
}
