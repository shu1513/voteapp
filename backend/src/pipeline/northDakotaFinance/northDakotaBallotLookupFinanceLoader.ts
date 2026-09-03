// North Dakota ballot-lookup finance loader: thin config wrapper over the
// standard loader. Phase 2 publishes total receipts, the donor-only "Raised"
// figure and contribution-size buckets; Phase 3 adds filed occupations for
// committees that pass the display gate (the sync stores none otherwise, so
// the card simply has no occupation rows). Spending (year-end statement
// only), cash (statewide filers only, unparsed) and outside totals (Phase 4)
// are never stored yet, so they surface as null with the coverage notes —
// never $0.

import type { Pool, PoolClient } from "pg";

import { isNorthDakotaCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { NORTH_DAKOTA_DIRECT_COVERAGE_NOTE } from "./northDakotaDirectContributionAggregator.js";
import { isNorthDakotaFinanceEligibleOffice } from "./northDakotaFinanceEligibleOffices.js";
import { NORTH_DAKOTA_CFRS_SOURCE_URL } from "./northDakotaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const NORTH_DAKOTA_OUTSIDE_COVERAGE_NOTE =
  "North Dakota independent expenditure filings have not been loaded yet, so outside spending totals are unavailable rather than zero.";

export async function loadNorthDakotaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "ND",
    source: "NORTH_DAKOTA_CFRS",
    sourceUrl: NORTH_DAKOTA_CFRS_SOURCE_URL,
    enabled: isNorthDakotaCampaignFinanceEnabled,
    isEligibleElection: (row) => isNorthDakotaFinanceEligibleOffice(officeInputFromElectionRow(row)),
    directBreakdownCategoryTypes: ["occupation", "contribution_size"],
    directCoverageNote: NORTH_DAKOTA_DIRECT_COVERAGE_NOTE,
    outsideCoverageNote: NORTH_DAKOTA_OUTSIDE_COVERAGE_NOTE,
    tables: {
      links: "nd_candidate_finance_links",
      summaries: "nd_candidate_finance_summaries",
      directBreakdowns: "nd_candidate_finance_direct_breakdowns",
      outsideGroups: "nd_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "nd_candidate_finance_outside_group_breakdowns",
    },
  });
}
