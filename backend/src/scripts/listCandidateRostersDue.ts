import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { STAGING_ITEM_TYPE_CANDIDATE_ROSTER } from "../config/electionsPipeline.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { usLatestLocalDateIso } from "../utils/usLocalDate.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Read-only work-queue query for the manual candidate-roster refresh loop:
// lists upcoming office elections whose roster staging item needs another
// research pass. A roster is due when it is
//   - 'no_results' (an earlier pass found no candidates — common early,
//     before anyone has announced), or
//   - 'written' with zero staged candidates in the payload (data debt from
//     before empty AI rosters were staged as no_results), or
//   - 'written' longer ago than the cooldown (a completed roster still needs
//     periodic reconciliation for late entrants near the election).
//
// Emptiness is judged on the staged payload, not on candidate_elections
// links: a written roster with staged candidates but zero links means the
// downstream profile/link fanout never completed — re-researching the roster
// would target the wrong pipeline stage. Those rosters are reported in the
// separate fanout-debt list instead.
//
// Rosters in pending/failed states belong to the initial research flow, not
// this refresh loop, so they are excluded. Refreshing far-future elections is
// wasted work — rosters change close to filing deadlines — so both lists are
// capped to elections within the lookahead window.

type Queryable = Pick<Pool, "query">;

export type CandidateRosterDueRow = {
  election_id: string;
  district_name: string | null;
  district_type: string | null;
  state: string | null;
  official_ballot_title: string | null;
  election_date: string;
  election_stage: string | null;
  roster_status: string;
  roster_written_at: string | null;
  staged_candidate_count: number;
  linked_candidate_count: number;
  reason: "no_results" | "empty_roster" | "stale";
};

export type CandidateRosterFanoutDebtRow = {
  election_id: string;
  district_name: string | null;
  district_type: string | null;
  state: string | null;
  official_ballot_title: string | null;
  election_date: string;
  election_stage: string | null;
  roster_written_at: string | null;
  staged_candidate_count: number;
};

export async function listCandidateRostersDue(
  db: Queryable,
  input: { asOfDate: string; cooldownDays: number; withinDays: number }
): Promise<CandidateRosterDueRow[]> {
  const result = await db.query<CandidateRosterDueRow>(
    `
      SELECT
        e.id::text AS election_id,
        d.name AS district_name,
        d.district_type,
        d.state,
        e.official_ballot_title,
        e.election_date::text AS election_date,
        e.election_stage::text AS election_stage,
        s.status AS roster_status,
        s.written_at::text AS roster_written_at,
        COALESCE(jsonb_array_length(s.payload->'candidates'), 0) AS staged_candidate_count,
        linked.linked_candidate_count,
        CASE
          WHEN s.status = 'no_results' THEN 'no_results'
          WHEN COALESCE(jsonb_array_length(s.payload->'candidates'), 0) = 0 THEN 'empty_roster'
          ELSE 'stale'
        END AS reason
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      JOIN public.staging_items AS s
        ON s.ingest_key = 'candidate_roster:' || e.id::text
       AND s.item_type = $4
      JOIN LATERAL (
        SELECT count(*)::int AS linked_candidate_count
        FROM public.candidate_elections AS ce
        JOIN public.candidates AS c
          ON c.id = ce.candidate_id
         AND c.deleted_at IS NULL
        WHERE ce.election_id = e.id
      ) AS linked ON true
      WHERE e.race_type = 'office'
        AND e.election_date >= $1::date
        AND (e.election_date - $1::date)::int <= $3::int
        AND s.status IN ('written', 'no_results')
        -- Staged candidates with zero links is fanout debt, not
        -- roster-research debt — the fanout-debt list owns those rows.
        AND NOT (
          COALESCE(jsonb_array_length(s.payload->'candidates'), 0) > 0
          AND linked.linked_candidate_count = 0
        )
        AND (
          s.status = 'no_results'
          OR COALESCE(jsonb_array_length(s.payload->'candidates'), 0) = 0
          OR s.written_at IS NULL
          -- AT TIME ZONE 'UTC' pins the cutoff: the as-of date is derived
          -- from UTC, and without the cast the timestamp-to-timestamptz
          -- comparison would shift with the session TimeZone.
          OR s.written_at < (($1::date - make_interval(days => $2::int)) AT TIME ZONE 'UTC')
        )
      ORDER BY e.election_date ASC, s.written_at ASC NULLS FIRST, e.id ASC
    `,
    [input.asOfDate, input.cooldownDays, input.withinDays, STAGING_ITEM_TYPE_CANDIDATE_ROSTER]
  );
  return result.rows;
}

// Written rosters whose staged candidates never became candidate_elections
// links: the profile/link fanout stalled or was never run. The remediation is
// manual:candidate-roster:fanout (or the profile pipeline), not another
// roster research pass, so these are kept out of the due list above.
export async function listCandidateRosterFanoutDebt(
  db: Queryable,
  input: { asOfDate: string; withinDays: number }
): Promise<CandidateRosterFanoutDebtRow[]> {
  const result = await db.query<CandidateRosterFanoutDebtRow>(
    `
      SELECT
        e.id::text AS election_id,
        d.name AS district_name,
        d.district_type,
        d.state,
        e.official_ballot_title,
        e.election_date::text AS election_date,
        e.election_stage::text AS election_stage,
        s.written_at::text AS roster_written_at,
        jsonb_array_length(s.payload->'candidates') AS staged_candidate_count
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      JOIN public.staging_items AS s
        ON s.ingest_key = 'candidate_roster:' || e.id::text
       AND s.item_type = $3
      JOIN LATERAL (
        SELECT count(*)::int AS linked_candidate_count
        FROM public.candidate_elections AS ce
        JOIN public.candidates AS c
          ON c.id = ce.candidate_id
         AND c.deleted_at IS NULL
        WHERE ce.election_id = e.id
      ) AS linked ON true
      WHERE e.race_type = 'office'
        AND e.election_date >= $1::date
        AND (e.election_date - $1::date)::int <= $2::int
        AND s.status = 'written'
        AND COALESCE(jsonb_array_length(s.payload->'candidates'), 0) > 0
        AND linked.linked_candidate_count = 0
      ORDER BY e.election_date ASC, s.written_at ASC NULLS FIRST, e.id ASC
    `,
    [input.asOfDate, input.withinDays, STAGING_ITEM_TYPE_CANDIDATE_ROSTER]
  );
  return result.rows;
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:candidate-roster:due", process.argv.slice(2), [
    { name: "--cooldown-days", value: "both" },
    { name: "--within-days", value: "both" },
  ]);
  loadProjectEnv();

  const cooldownDays = readPositiveIntegerFlag(process.argv.slice(2), "--cooldown-days", 30);
  const withinDays = readPositiveIntegerFlag(process.argv.slice(2), "--within-days", 90);
  // US-local boundary, not UTC: after UTC midnight a UTC date would drop
  // elections still happening "today" in western states.
  const asOfDate = usLatestLocalDateIso();

  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for the candidate-roster due list");
  }
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const due = await listCandidateRostersDue(pool, { asOfDate, cooldownDays, withinDays });
    const fanoutDebt = await listCandidateRosterFanoutDebt(pool, { asOfDate, withinDays });

    console.log(
      JSON.stringify(
        {
          asOfDate,
          cooldownDays,
          withinDays,
          dueCount: due.length,
          due,
          fanoutDebtCount: fanoutDebt.length,
          fanoutDebt,
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
    console.error("candidate-roster due list failed:", message);
    process.exitCode = 1;
  });
}
