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
 * Usage: npm run manual:records:audit
 */

type AuditRow = {
  candidate_id: string;
  display_name: string;
  current_office: string | null;
  is_incumbent: boolean;
  last_records_searched_at: string;
  election_titles: string[];
};

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
          array_remove(array_agg(DISTINCT e.official_ballot_title), NULL) AS election_titles
        FROM public.candidates c
        LEFT JOIN public.candidate_elections ce ON ce.candidate_id = c.id
        LEFT JOIN public.elections e ON e.id = ce.election_id
        WHERE c.deleted_at IS NULL
          AND c.merged_into_candidate_id IS NULL
          AND c.last_records_searched_at IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM public.candidate_records r WHERE r.candidate_id = c.id
          )
        GROUP BY c.id, c.display_name, c.current_office, c.last_records_searched_at
        HAVING (c.current_office IS NOT NULL OR coalesce(bool_or(ce.is_incumbent), false))
        ORDER BY c.display_name ASC
      `
    );

    console.log(
      JSON.stringify(
        {
          suspectCount: result.rows.length,
          explanation:
            "Candidates with a records-search completion stamp, a current office or incumbent election link, and ZERO candidate_records rows. Each needs a proper per-question record sweep re-run (the stamp may be a false no_records_found).",
          suspects: result.rows.map((row) => ({
            candidateId: row.candidate_id,
            displayName: row.display_name,
            currentOffice: row.current_office,
            isIncumbent: row.is_incumbent,
            lastRecordsSearchedAt: row.last_records_searched_at,
            electionTitles: row.election_titles,
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
