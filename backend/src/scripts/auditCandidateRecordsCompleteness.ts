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
 * Usage: npm run manual:records:audit
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
};

const NO_RECORDS_FOUND_GAP_ID = "candidate_records.no_records_found";

function isConfirmedNull(row: AuditRow): boolean {
  return (row.confirmed_gap_ids ?? []).includes(NO_RECORDS_FOUND_GAP_ID);
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:records:audit", process.argv.slice(2), []);
  loadProjectEnv();

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
          jsonb_array_length(sc.evidence -> 'entries')::int AS evidence_entry_count
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
        GROUP BY c.id, c.display_name, c.current_office, c.last_records_searched_at,
          sc.confirmed_gap_ids, sc.confirmed_at, sc.evidence
        HAVING (c.current_office IS NOT NULL OR coalesce(bool_or(ce.is_incumbent), false))
        ORDER BY c.display_name ASC
      `
    );

    const suspects = result.rows.filter((row) => !isConfirmedNull(row));
    const confirmedNulls = result.rows.filter((row) => isConfirmedNull(row));

    console.log(
      JSON.stringify(
        {
          suspectCount: suspects.length,
          confirmedNullCount: confirmedNulls.length,
          explanation:
            "Suspects: candidates with a records-search completion stamp, a current office or incumbent election link, ZERO candidate_records rows, and NO persisted sweep confirmation. Each needs a proper per-question record sweep re-run (the stamp may be a false no_records_found). Confirmed nulls carry an evidence-backed candidate_record_sweep_confirmations row and need no re-run.",
          suspects: suspects.map((row) => ({
            candidateId: row.candidate_id,
            displayName: row.display_name,
            currentOffice: row.current_office,
            isIncumbent: row.is_incumbent,
            lastRecordsSearchedAt: row.last_records_searched_at,
            electionTitles: row.election_titles,
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
