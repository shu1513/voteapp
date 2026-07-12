import type { Pool, PoolClient } from "pg";
import { isHoustonCampaignFinanceEnabled } from "../../config/featureFlags.js";
import type {
  BallotLookupFinanceSummary,
  StateFinanceRequestCandidateRow,
  StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { isHoustonFinanceEligibleElection } from "./houstonFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export async function loadHoustonCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "TX",
    source: "HOUSTON_CAMPAIGN_FINANCE",
    sourceUrl: "https://www.houstontx.gov/campaignfinance/",
    enabled: isHoustonCampaignFinanceEnabled,
    isEligibleElection: (row) => isHoustonFinanceEligibleElection({
      state: row.state,
      districtType: row.district_type,
      geoidCompact: row.geoid_compact,
      officeScope: row.office_scope,
      officeCanonicalName: row.office_canonical_name,
    }),
    tables: {
      links: "hou_candidate_finance_links",
      summaries: "hou_candidate_finance_summaries",
      directBreakdowns: "hou_candidate_finance_direct_breakdowns",
      outsideGroups: "hou_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "hou_candidate_finance_outside_group_breakdowns",
    },
  });
}
