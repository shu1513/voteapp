import type { Pool, PoolClient } from "pg";

import { isOhioCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { isOhioFinanceEligibleOffice } from "./ohioFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const GENERIC_OHIO_SOS_SOURCE_URL = "https://www.ohiosos.gov/campaign-finance/search/";

export async function loadOhioCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "OH",
    source: "OHIO_SOS",
    sourceUrl: GENERIC_OHIO_SOS_SOURCE_URL,
    enabled: isOhioCampaignFinanceEnabled,
    isEligibleElection: (row) => isOhioFinanceEligibleOffice(officeInputFromElectionRow(row)),
    evidenceLabelTypes: ["donor"],
    tables: {
      links: "oh_candidate_finance_links",
      summaries: "oh_candidate_finance_summaries",
      directBreakdowns: "oh_candidate_finance_direct_breakdowns",
      outsideGroups: "oh_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "oh_candidate_finance_outside_group_breakdowns",
    },
  });
}
