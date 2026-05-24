import type { Pool } from "pg";

import type { ElectionDistrictType } from "../../types/election.js";
import type { ElectionsSearchPolicy } from "./electionsSearchPolicy.js";

export type ElectionsSearchEligibilityReason =
  | "never_searched"
  | "cooldown_not_elapsed"
  | "due_no_upcoming"
  | "not_due";

export type DistrictElectionSearchFacts = {
  district_id: string;
  district_name: string;
  district_type: ElectionDistrictType;
  state: string;
  last_elections_searched_at: string | null;
  max_known_election_date: string | null;
  has_upcoming: boolean;
};

export type DistrictElectionSearchEligibilityRow = DistrictElectionSearchFacts & {
  reason: ElectionsSearchEligibilityReason;
};

const DISTRICT_ELECTION_FACTS_SQL = `
  SELECT
    d.id AS district_id,
    d.name AS district_name,
    d.district_type,
    d.state,
    d.last_elections_searched_at::text AS last_elections_searched_at,
    max(e.election_date)::text AS max_known_election_date,
    COALESCE(bool_or(e.election_date >= $1::date), false) AS has_upcoming
  FROM public.districts AS d
  LEFT JOIN public.elections AS e
    ON e.district_id = d.id
  GROUP BY d.id, d.name, d.district_type, d.state, d.last_elections_searched_at
  ORDER BY d.last_elections_searched_at NULLS FIRST, d.id
`;

function addDays(date: string, days: number): string {
  const base = new Date(`${date}T00:00:00.000Z`);
  base.setUTCDate(base.getUTCDate() + days);
  return base.toISOString().slice(0, 10);
}

export function classifyDistrictElectionSearchEligibility(
  facts: DistrictElectionSearchFacts,
  policy: Pick<ElectionsSearchPolicy, "asOfDate" | "cooldownDays">
): ElectionsSearchEligibilityReason {
  if (!facts.last_elections_searched_at) {
    return "never_searched";
  }

  const lastSearchedDate = facts.last_elections_searched_at.slice(0, 10);
  if (addDays(lastSearchedDate, policy.cooldownDays) > policy.asOfDate) {
    return "cooldown_not_elapsed";
  }

  if (!facts.has_upcoming) {
    return "due_no_upcoming";
  }

  return "not_due";
}

export async function listDistrictElectionSearchEligibility(
  pool: Pool,
  policy: Pick<ElectionsSearchPolicy, "asOfDate" | "cooldownDays">
): Promise<DistrictElectionSearchEligibilityRow[]> {
  const result = await pool.query<DistrictElectionSearchFacts>(DISTRICT_ELECTION_FACTS_SQL, [policy.asOfDate]);
  return result.rows.map((row) => ({
    ...row,
    reason: classifyDistrictElectionSearchEligibility(row, policy),
  }));
}
