import type { Pool, PoolClient } from "pg";

import { isMaineCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { isMaineFinanceEligibleOffice } from "./maineFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const GENERIC_MAINE_CFIS_SOURCE_URL = "https://mainecampaignfinance.com/";

export async function loadMaineCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "ME",
    source: "MAINE_CFIS",
    sourceUrl: GENERIC_MAINE_CFIS_SOURCE_URL,
    enabled: isMaineCampaignFinanceEnabled,
    isEligibleElection: (row) => isMaineFinanceEligibleOffice(officeInputFromElectionRow(row)),
    evidenceLabelTypes: ["donor"],
    tables: {
      links: "me_candidate_finance_links",
      summaries: "me_candidate_finance_summaries",
      directBreakdowns: "me_candidate_finance_direct_breakdowns",
      outsideGroups: "me_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "me_candidate_finance_outside_group_breakdowns",
    },
  });
}
