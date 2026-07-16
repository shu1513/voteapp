import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";

import { assertKnownCliFlags } from "./manualCliFlags.js";
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
  if (filters.electionId) {
    push(
      filters.electionId,
      (p) =>
        `EXISTS (SELECT 1 FROM public.candidate_elections cef WHERE cef.candidate_id = c.id AND cef.election_id = ${p}::uuid)`
    );
  }
  if (filters.districtId) {
    push(
      filters.districtId,
      (p) =>
        `EXISTS (SELECT 1 FROM public.candidate_elections cef JOIN public.elections ef ON ef.id = cef.election_id WHERE cef.candidate_id = c.id AND ef.district_id = ${p}::uuid)`
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
        LEFT JOIN public.candidate_elections ce ON ce.candidate_id = c.id
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
