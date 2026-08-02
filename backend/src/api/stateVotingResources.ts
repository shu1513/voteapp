import type { Pool, PoolClient } from "pg";

type Queryable = Pick<Pool | PoolClient, "query">;

/**
 * The slice of state_resources the "How to vote" UI needs: the official
 * polling-place lookup plus the official mail-ballot request destination
 * (with its type and request deadline). Row shape mirrors the table columns
 * so the frontend vocabulary stays identical to the researched data.
 */
export type StateVotingResources = {
  state_abbreviation: string;
  state_name: string;
  polling_place_url: string;
  mail_voting_available: boolean;
  mail_ballot_request_url: string | null;
  mail_ballot_request_type: "online_portal" | "form" | "instructions" | "not_required" | null;
  mail_ballot_request_deadline_rule: string | null;
};

export type StateVotingResourcesResult = {
  state_resources: StateVotingResources;
};

export async function getStateVotingResources(
  db: Queryable,
  stateAbbreviation: string
): Promise<StateVotingResourcesResult | null> {
  const result = await db.query<StateVotingResources>(
    `
      SELECT
        state_abbreviation,
        state_name,
        polling_place_url,
        mail_voting_available,
        mail_ballot_request_url,
        mail_ballot_request_type,
        mail_ballot_request_deadline_rule
      FROM public.state_resources
      WHERE state_abbreviation = $1
    `,
    [stateAbbreviation]
  );

  const row = result.rows[0];
  return row ? { state_resources: row } : null;
}
