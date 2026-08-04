import type { Pool, PoolClient } from "pg";

import { isDistrictOfColumbiaCampaignFinanceEnabled } from "../../config/featureFlags.js";
import type {
  BallotLookupFinanceSummary,
  StateFinanceRequestCandidateRow,
  StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const GENERIC_DISTRICT_OF_COLUMBIA_OCF_SOURCE_URL = "https://efiling.ocf.dc.gov/DataDownload";

export async function loadDistrictOfColumbiaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "DC",
    source: "DISTRICT_OF_COLUMBIA_OCF",
    sourceUrl: GENERIC_DISTRICT_OF_COLUMBIA_OCF_SOURCE_URL,
    enabled: isDistrictOfColumbiaCampaignFinanceEnabled,
    // OCF keys every relation by committee_key, so the one-column option
    // covers the link and both outside identities (illinois precedent);
    // everything else is shared defaults — both evidence label types, no
    // office filter.
    //
    // Deliberate behavior alignment, not a preservation: the bespoke mapper
    // hardcoded organization_type: "donor" on evidence rows even though its
    // SQL selects donor AND employer rows, so employer evidence was
    // mislabeled and its explanation used donor wording. The shared mapper
    // reads the row's category_type (the c22c24f2 fix DC's copy never got),
    // matching every other dual-label state.
    committeeColumn: "committee_key",
    tables: {
      links: "dc_candidate_finance_links",
      summaries: "dc_candidate_finance_summaries",
      directBreakdowns: "dc_candidate_finance_direct_breakdowns",
      outsideGroups: "dc_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "dc_candidate_finance_outside_group_breakdowns",
    },
  });
}
