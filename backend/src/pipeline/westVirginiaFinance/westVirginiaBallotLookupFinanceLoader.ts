// West Virginia ballot-lookup finance loader: thin config wrapper over the
// standard loader. cash_on_hand and outside totals are never stored in
// Phase 1 (covers unparsed; F-7b independent-expenditure reports are
// scanned PDFs awaiting the document phase), so they surface as null with
// the coverage notes — never $0. Direct breakdowns carry occupation (the
// state's own labels), industry (from the pre-2027 employer field) and size
// buckets.

import type { Pool, PoolClient } from "pg";

import { isWestVirginiaCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { WEST_VIRGINIA_DIRECT_COVERAGE_NOTE } from "./westVirginiaDirectContributionAggregator.js";
import { isWestVirginiaFinanceEligibleOffice } from "./westVirginiaFinanceEligibleOffices.js";
import { WEST_VIRGINIA_CFRS_SOURCE_URL } from "./westVirginiaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const WEST_VIRGINIA_OUTSIDE_COVERAGE_NOTE =
  "West Virginia independent expenditure reports are filed as scanned paper forms that have not been extracted yet, so outside spending totals are unavailable rather than zero.";

export async function loadWestVirginiaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "WV",
    source: "WEST_VIRGINIA_CFRS",
    sourceUrl: WEST_VIRGINIA_CFRS_SOURCE_URL,
    enabled: isWestVirginiaCampaignFinanceEnabled,
    isEligibleElection: (row) => isWestVirginiaFinanceEligibleOffice(officeInputFromElectionRow(row)),
    directBreakdownCategoryTypes: ["occupation", "industry", "contribution_size"],
    directCoverageNote: WEST_VIRGINIA_DIRECT_COVERAGE_NOTE,
    outsideCoverageNote: WEST_VIRGINIA_OUTSIDE_COVERAGE_NOTE,
    tables: {
      links: "wv_candidate_finance_links",
      summaries: "wv_candidate_finance_summaries",
      directBreakdowns: "wv_candidate_finance_direct_breakdowns",
      outsideGroups: "wv_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "wv_candidate_finance_outside_group_breakdowns",
    },
  });
}
