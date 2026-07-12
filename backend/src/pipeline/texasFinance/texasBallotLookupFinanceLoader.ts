import type { Pool, PoolClient } from "pg";
import { isTexasCampaignFinanceEnabled } from "../../config/featureFlags.js";
import type {
  BallotLookupFinanceSummary,
  StateFinanceRequestCandidateRow,
  StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const HOUSTON_LOCAL_FINANCE_OFFICES = new Set(["Mayor", "City Controller", "City Council Member"]);

export async function loadTexasCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "TX",
    source: "TEXAS_TEC",
    sourceUrl: "https://www.ethics.state.tx.us/search/cf/",
    enabled: isTexasCampaignFinanceEnabled,
    isEligibleElection: (row) => !(
      row.office_scope?.trim() === "place" &&
      HOUSTON_LOCAL_FINANCE_OFFICES.has(row.office_canonical_name?.trim() ?? "")
    ),
    tables: {
      links: "tx_candidate_finance_links",
      summaries: "tx_candidate_finance_summaries",
      directBreakdowns: "tx_candidate_finance_direct_breakdowns",
      outsideGroups: "tx_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "tx_candidate_finance_outside_group_breakdowns",
    },
  });
}
