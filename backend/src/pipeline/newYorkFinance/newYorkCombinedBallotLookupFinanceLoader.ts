import type { Pool, PoolClient } from "pg";

import {
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadNewYorkCityCandidateFinanceSummariesByCandidateElection } from "../newYorkCityFinance/newYorkCityBallotLookupFinanceLoader.js";
import { loadNewYorkCandidateFinanceSummariesByCandidateElection } from "./newYorkBallotLookupFinanceLoader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export function mergeNewYorkFinanceSummaryMaps(input: {
  state: ReadonlyMap<string, BallotLookupFinanceSummary>;
  city: ReadonlyMap<string, BallotLookupFinanceSummary>;
  onCollision?: (key: string) => void;
}): Map<string, BallotLookupFinanceSummary> {
  const merged = new Map(input.state);
  const onCollision = input.onCollision ?? ((key: string) => {
    console.warn(`Duplicate NY finance summary key; preferring NEW_YORK_CITY_CFB: ${key}`);
  });
  for (const [key, summary] of input.city) {
    if (merged.has(key)) {
      onCollision(key);
    }
    merged.set(key, summary);
  }
  return merged;
}

export async function loadCombinedNewYorkCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  const state = await loadNewYorkCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const city = await loadNewYorkCityCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  return mergeNewYorkFinanceSummaryMaps({ state, city });
}
