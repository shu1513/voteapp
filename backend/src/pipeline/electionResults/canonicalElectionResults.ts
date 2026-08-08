import type { Pool, PoolClient } from "pg";

type Queryable = Pick<Pool | PoolClient, "query">;

export type CanonicalElectionResultWinner = {
  candidate_id?: string;
  candidate_name?: string;
  party?: string;
};

export type CanonicalElectionResult = {
  outcome: string;
  winners: CanonicalElectionResultWinner[];
};

type CanonicalResultRow = {
  election_id: string;
  outcome: string;
  winners: unknown;
};

// User-payload winner shape: id (pick matching), name, party — deliberately
// NOT candidate_election_id, which is pipeline plumbing. Malformed entries
// drop out rather than failing the whole read.
function parseWinners(raw: unknown): CanonicalElectionResultWinner[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.flatMap((item): CanonicalElectionResultWinner[] => {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      return [];
    }
    const row = item as Record<string, unknown>;
    const winner: CanonicalElectionResultWinner = {};
    for (const key of ["candidate_id", "candidate_name", "party"] as const) {
      const value = row[key];
      if (typeof value === "string" && value.trim().length > 0) {
        winner[key] = value.trim();
      }
    }
    return Object.keys(winner).length > 0 ? [winner] : [];
  });
}

/**
 * One canonical result per election, keyed by election id — the result a
 * user-facing surface should present as THE call. Same ranking as the ballot
 * summary's resultSummaryResult (ballotLookup.ts, which keeps its own copy
 * because its winners keep pipeline fields this parser strips): certified
 * beats election_night, freshest retrieved_at wins, and outcome = 'unknown'
 * rows (not_found / not_final_yet sweeps) are filtered out — a later unknown
 * row must not shadow a decisive call. Ballot measures rank through the same
 * window with an empty winner set (passed/failed is the whole answer).
 */
export async function loadCanonicalElectionResults(
  db: Queryable,
  electionIds: readonly string[]
): Promise<Map<string, CanonicalElectionResult>> {
  if (electionIds.length === 0) {
    return new Map();
  }
  const result = await db.query<CanonicalResultRow>(
    `
      WITH all_results AS (
        SELECT
          er.election_id,
          er.outcome,
          er.winners,
          er.pass_type,
          er.retrieved_at
        FROM public.election_results AS er
        WHERE er.election_id = ANY($1::uuid[])
          AND er.outcome <> 'unknown'

        UNION ALL

        SELECT
          bm.election_id,
          bmr.outcome,
          '[]'::jsonb AS winners,
          bmr.pass_type,
          bmr.retrieved_at
        FROM public.ballot_measure_results AS bmr
        JOIN public.ballot_measures AS bm
          ON bm.id = bmr.ballot_measure_id
        WHERE bm.election_id = ANY($1::uuid[])
          AND bmr.outcome <> 'unknown'
      ),
      ranked AS (
        SELECT
          election_id,
          outcome,
          winners,
          row_number() OVER (
            PARTITION BY election_id
            ORDER BY
              CASE pass_type
                WHEN 'certified' THEN 1
                WHEN 'election_night' THEN 2
                ELSE 3
              END,
              retrieved_at DESC,
              outcome ASC
          ) AS rn
        FROM all_results
      )
      SELECT election_id::text AS election_id, outcome, winners
      FROM ranked
      WHERE rn = 1
    `,
    [electionIds]
  );
  return new Map(
    result.rows.map((row) => [row.election_id, { outcome: row.outcome, winners: parseWinners(row.winners) }])
  );
}
