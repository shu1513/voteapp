import type { Pool, PoolClient } from "pg";

import { isWisconsinCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { isWisconsinFinanceEligibleOffice } from "./wisconsinFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const GENERIC_WISCONSIN_SUNSHINE_SOURCE_URL = "https://campaignfinance.wi.gov/";

export async function loadWisconsinCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "WI",
    source: "WISCONSIN_SUNSHINE",
    sourceUrl: GENERIC_WISCONSIN_SUNSHINE_SOURCE_URL,
    enabled: isWisconsinCampaignFinanceEnabled,
    isEligibleElection: (row) => isWisconsinFinanceEligibleOffice(officeInputFromElectionRow(row)),
    // Wisconsin's outside tables use sponsor_id/sponsor_name (link table stays
    // canonical). The bespoke summary query carried an explanatory SQL comment;
    // dropping it is text-only.
    outsideGroupIdentityColumns: { id: "sponsor_id", name: "sponsor_name" },
    evidenceLabelTypes: ["donor"],
    tables: {
      links: "wi_candidate_finance_links",
      summaries: "wi_candidate_finance_summaries",
      directBreakdowns: "wi_candidate_finance_direct_breakdowns",
      outsideGroups: "wi_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "wi_candidate_finance_outside_group_breakdowns",
    },
  });
}
