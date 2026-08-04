import type { Pool, PoolClient } from "pg";

import { isWashingtonCampaignFinanceEnabled } from "../../config/featureFlags.js";
import type {
  BallotLookupFinanceSummary,
  StateFinanceRequestCandidateRow,
  StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const GENERIC_WASHINGTON_PDC_SOURCE_URL = "https://www.pdc.wa.gov/political-disclosure-reporting-data/browse-search-data";

export async function loadWashingtonCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "WA",
    source: "WASHINGTON_PDC",
    sourceUrl: GENERIC_WASHINGTON_PDC_SOURCE_URL,
    enabled: isWashingtonCampaignFinanceEnabled,
    // Washington's link table keeps the canonical committee_id, but its
    // outside tables use sponsor_id/sponsor_name — the mixed-identity case the
    // per-relation descriptor exists for. Evidence uses both label types (the
    // shared default).
    outsideGroupIdentityColumns: { id: "sponsor_id", name: "sponsor_name" },
    tables: {
      links: "wa_candidate_finance_links",
      summaries: "wa_candidate_finance_summaries",
      directBreakdowns: "wa_candidate_finance_direct_breakdowns",
      outsideGroups: "wa_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "wa_candidate_finance_outside_group_breakdowns",
    },
  });
}
