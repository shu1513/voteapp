import type { Pool, PoolClient } from "pg";

import { isIllinoisCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { isIllinoisFinanceEligibleOffice } from "./illinoisFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const GENERIC_ILLINOIS_SBE_SOURCE_URL = "https://www.elections.il.gov/CampaignDisclosure/";

export async function loadIllinoisCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "IL",
    source: "ILLINOIS_SBE",
    sourceUrl: GENERIC_ILLINOIS_SBE_SOURCE_URL,
    enabled: isIllinoisCampaignFinanceEnabled,
    isEligibleElection: (row) => isIllinoisFinanceEligibleOffice(officeInputFromElectionRow(row)),
    committeeColumn: "committee_key",
    summaryVariant: "illinoisD2",
    evidenceLabelTypes: ["donor"],
    tables: {
      links: "il_candidate_finance_links",
      summaries: "il_candidate_finance_summaries",
      directBreakdowns: "il_candidate_finance_direct_breakdowns",
      outsideGroups: "il_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "il_candidate_finance_outside_group_breakdowns",
    },
  });
}
