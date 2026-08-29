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

// Wording is careful not to overclaim: a candidate with a single filed
// report has no consecutive-report boundary to verify, so the chain claim
// is scoped to where consecutive reports exist. Itemized totals are always
// cross-checked between the report-detail and export surfaces.
const MONTANA_DIRECT_COVERAGE_NOTE =
  "Totals are summed from itemized Montana CERS filings and cross-checked across the state's disclosure surfaces; where consecutive reports exist they are verified against the official cash-balance chain, which also supplies derived unitemized small-donor amounts.";

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
