import type { Pool, PoolClient } from "pg";

import { isGeorgiaCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { isGeorgiaFinanceEligibleOffice } from "./georgiaFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const GENERIC_GEORGIA_ETHICS_SOURCE_URL = "https://ethics.ga.gov/records-search-all/";

// georgia_plan.md D6/D12: Georgia discloses no per-target amount on an
// independent expenditure, so a transaction naming several candidates at
// once has no defensible split and stays out of the per-candidate totals
// (49 of 387 IE transactions in the 2026 filing-year probe). That is a
// systematic gap, not a rounding error, so it is stated with the totals
// until the source discloses target-level amounts — at which point this
// note is removed.
const GEORGIA_OUTSIDE_COVERAGE_NOTE =
  "Covers independent expenditures that name a single candidate, as reported to the Georgia Government " +
  "Transparency and Campaign Finance Commission. Spending that names several candidates in one expenditure " +
  "is not included yet.";

export async function loadGeorgiaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "GA",
    source: "GEORGIA_ETHICS",
    sourceUrl: GENERIC_GEORGIA_ETHICS_SOURCE_URL,
    enabled: isGeorgiaCampaignFinanceEnabled,
    isEligibleElection: (row) => isGeorgiaFinanceEligibleOffice(officeInputFromElectionRow(row)),
    evidenceLabelTypes: ["donor"],
    outsideCoverageNote: GEORGIA_OUTSIDE_COVERAGE_NOTE,
    tables: {
      links: "ga_candidate_finance_links",
      summaries: "ga_candidate_finance_summaries",
      directBreakdowns: "ga_candidate_finance_direct_breakdowns",
      outsideGroups: "ga_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "ga_candidate_finance_outside_group_breakdowns",
    },
  });
}
