import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Read-only work-queue query for the manual followed-candidate records
// refresh: lists candidates that (a) have at least one follower — any
// follower, regardless of the follow's email-notification toggles, because
// the in-app "new records" surface serves non-subscribers too — and (b) are
// past the records-search cooldown. Each row carries the latest office
// election context the manual records writer needs, plus
// last_records_researched_through as the delta-window start (null means the
// candidate needs a full sweep, not a windowed one).
//
// Followed candidates with no office election link cannot go through the
// records writer at all (it requires a candidate/election office context),
// so they are reported in a separate list instead of silently vanishing.

type Queryable = Pick<Pool, "query">;

export type FollowedCandidateRecordsDueRow = {
  candidate_id: string;
  display_name: string | null;
  state: string | null;
  party: string | null;
  current_office: string | null;
  follower_count: number;
  last_records_searched_at: string | null;
  /** Delta-window start for --since-date; null = never researched, run a full sweep. */
  last_records_researched_through: string | null;
  election_id: string;
  official_ballot_title: string | null;
  election_date: string;
};

export type FollowedCandidateWithoutOfficeElectionRow = {
  candidate_id: string;
  display_name: string | null;
  state: string | null;
  follower_count: number;
  last_records_searched_at: string | null;
};

export async function listFollowedCandidateRecordsDue(
  db: Queryable,
  input: { asOfDate: string; cooldownDays: number }
): Promise<FollowedCandidateRecordsDueRow[]> {
  const result = await db.query<FollowedCandidateRecordsDueRow>(
    `
      WITH followed_candidate AS (
        SELECT
          c.id,
          COALESCE(
            NULLIF(trim(c.display_name), ''),
            trim(concat_ws(' ', c.first_name, c.last_name))
          ) AS display_name,
          c.state,
          c.party,
          c.current_office,
          c.last_records_searched_at,
          c.last_records_researched_through,
          follower.follower_count
        FROM public.candidates AS c
        JOIN LATERAL (
          SELECT count(*)::int AS follower_count
          FROM public.user_candidate_follows AS f
          JOIN public.users AS u
            ON u.id = f.user_id
           AND u.deleted_at IS NULL
          WHERE f.candidate_id = c.id
        ) AS follower ON follower.follower_count > 0
        WHERE c.deleted_at IS NULL
          AND c.merged_into_candidate_id IS NULL
          AND (
            c.last_records_searched_at IS NULL
            OR c.last_records_searched_at < ($1::date - make_interval(days => $2::int))
          )
      ),
      ranked_election AS (
        SELECT
          ce.candidate_id,
          ce.election_id,
          e.official_ballot_title,
          e.election_date,
          row_number() OVER (
            PARTITION BY ce.candidate_id
            ORDER BY e.election_date DESC, ce.created_at DESC, ce.id DESC
          ) AS candidate_rank
        FROM public.candidate_elections AS ce
        JOIN public.elections AS e
          ON e.id = ce.election_id
        WHERE e.race_type = 'office'
          AND ce.candidate_id IN (SELECT id FROM followed_candidate)
      )
      SELECT
        fc.id::text AS candidate_id,
        fc.display_name,
        fc.state,
        fc.party,
        fc.current_office,
        fc.follower_count,
        fc.last_records_searched_at::text AS last_records_searched_at,
        fc.last_records_researched_through::text AS last_records_researched_through,
        re.election_id::text AS election_id,
        re.official_ballot_title,
        re.election_date::text AS election_date
      FROM followed_candidate AS fc
      JOIN ranked_election AS re
        ON re.candidate_id = fc.id
       AND re.candidate_rank = 1
      ORDER BY fc.last_records_searched_at ASC NULLS FIRST, fc.id ASC
    `,
    [input.asOfDate, input.cooldownDays]
  );
  return result.rows;
}

export async function listFollowedCandidatesWithoutOfficeElection(
  db: Queryable
): Promise<FollowedCandidateWithoutOfficeElectionRow[]> {
  const result = await db.query<FollowedCandidateWithoutOfficeElectionRow>(
    `
      SELECT
        c.id::text AS candidate_id,
        COALESCE(
          NULLIF(trim(c.display_name), ''),
          trim(concat_ws(' ', c.first_name, c.last_name))
        ) AS display_name,
        c.state,
        follower.follower_count,
        c.last_records_searched_at::text AS last_records_searched_at
      FROM public.candidates AS c
      JOIN LATERAL (
        SELECT count(*)::int AS follower_count
        FROM public.user_candidate_follows AS f
        JOIN public.users AS u
          ON u.id = f.user_id
         AND u.deleted_at IS NULL
        WHERE f.candidate_id = c.id
      ) AS follower ON follower.follower_count > 0
      WHERE c.deleted_at IS NULL
        AND c.merged_into_candidate_id IS NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.candidate_elections AS ce
          JOIN public.elections AS e
            ON e.id = ce.election_id
          WHERE ce.candidate_id = c.id
            AND e.race_type = 'office'
        )
      ORDER BY display_name ASC, c.id ASC
    `
  );
  return result.rows;
}

export function readCooldownDaysDefault(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env.CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS?.trim();
  if (!raw) {
    return 30;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS: ${raw}. Expected a positive integer.`);
  }
  return Number(raw);
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:candidate-records:due", process.argv.slice(2), [
    { name: "--cooldown-days", value: "both" },
  ]);
  loadProjectEnv();

  const cooldownDays = readPositiveIntegerFlag(
    process.argv.slice(2),
    "--cooldown-days",
    readCooldownDaysDefault()
  );
  const asOfDate = new Date().toISOString().slice(0, 10);

  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for the followed-candidate records due list");
  }
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const due = await listFollowedCandidateRecordsDue(pool, { asOfDate, cooldownDays });
    const followedWithoutOfficeElection = await listFollowedCandidatesWithoutOfficeElection(pool);

    console.log(
      JSON.stringify(
        {
          asOfDate,
          cooldownDays,
          dueCount: due.length,
          due,
          followedWithoutOfficeElectionCount: followedWithoutOfficeElection.length,
          followedWithoutOfficeElection,
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
    console.error("followed-candidate records due list failed:", message);
    process.exitCode = 1;
  });
}
