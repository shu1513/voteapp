import type { PoolClient } from "pg";

type Queryable = Pick<PoolClient, "query">;

export type CandidateElectionOfficeContext = {
  candidateId: string;
  candidateDisplayName: string;
  electionId: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage: string | null;
  senateClass: string | null;
  termEndYear: string | null;
  officeId: string;
  electionSources: unknown;
};

export async function loadCandidateElectionOfficeContext(
  client: Queryable,
  candidateId: string,
  electionId: string
): Promise<CandidateElectionOfficeContext | null> {
  const result = await client.query<CandidateElectionOfficeContext>(
    `
      SELECT
        c.id AS "candidateId",
        COALESCE(NULLIF(trim(c.display_name), ''), trim(c.first_name || ' ' || c.last_name)) AS "candidateDisplayName",
        e.id AS "electionId",
        d.name AS "districtName",
        d.district_type AS "districtType",
        d.state AS "state",
        e.election_date::text AS "electionDate",
        e.official_ballot_title AS "officialBallotTitle",
        e.election_stage::text AS "electionStage",
        sm.senate_class AS "senateClass",
        sm.term_end_year AS "termEndYear",
        e.office_id::text AS "officeId",
        e.sources AS "electionSources"
      FROM public.candidate_elections ce
      JOIN public.candidates c
        ON c.id = ce.candidate_id
      JOIN public.elections e
        ON e.id = ce.election_id
      JOIN public.districts d
        ON d.id = e.district_id
      LEFT JOIN public.election_senate_metadata sm
        ON sm.election_id = e.id
      WHERE ce.candidate_id = $1
        AND ce.election_id = $2
        AND c.deleted_at IS NULL
        AND e.office_id IS NOT NULL
      LIMIT 1
    `,
    [candidateId, electionId]
  );

  return result.rows[0] ?? null;
}
