import type { Pool, PoolClient } from "pg";

import { isMontanaCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { MONTANA_CERS_DASHBOARD_URL } from "./montanaCersClient.js";
import { isMontanaFinanceEligibleOffice } from "./montanaFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const MONTANA_DIRECT_COVERAGE_NOTE =
  "Totals are summed from itemized Montana CERS filings and verified against each report's official cash-balance chain; unitemized small-donor amounts are derived from that chain.";

export async function loadMontanaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "MT",
    source: "MONTANA_COPP",
    sourceUrl: MONTANA_CERS_DASHBOARD_URL,
    enabled: isMontanaCampaignFinanceEnabled,
    isEligibleElection: (row) => isMontanaFinanceEligibleOffice(officeInputFromElectionRow(row)),
    // Phase 2b outside funders are organizational donors only (MO pattern).
    evidenceLabelTypes: ["donor"],
    directCoverageNote: MONTANA_DIRECT_COVERAGE_NOTE,
    tables: {
      links: "mt_candidate_finance_links",
      summaries: "mt_candidate_finance_summaries",
      directBreakdowns: "mt_candidate_finance_direct_breakdowns",
      outsideGroups: "mt_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "mt_candidate_finance_outside_group_breakdowns",
    },
  });
}
