import type { Pool, PoolClient } from "pg";

import {
  mergeFinanceSummaryMapsStrict,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadNewYorkCityCandidateFinanceSummariesByCandidateElection } from "../newYorkCityFinance/newYorkCityBallotLookupFinanceLoader.js";
import { loadNewYorkCandidateFinanceSummariesByCandidateElection } from "./newYorkBallotLookupFinanceLoader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export async function loadCombinedNewYorkCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  const state = await loadNewYorkCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  const city = await loadNewYorkCityCandidateFinanceSummariesByCandidateElection(db, candidateRows, electionRows);
  return mergeFinanceSummaryMapsStrict([
    { source: "NEW_YORK_SODA", summaries: state },
    { source: "NEW_YORK_CITY_CFB", summaries: city },
  ]);
}
