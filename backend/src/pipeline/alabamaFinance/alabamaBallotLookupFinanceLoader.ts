// Alabama ballot-lookup finance loader: thin config wrapper over the
// standard loader. Outside totals are never stored (Alabama publishes no
// independent-expenditure targeting data), so they surface as null with the
// coverage note — never $0. The direct note is the source-wide static one
// from the aggregator; the standard family persists no per-candidate note.

import type { Pool, PoolClient } from "pg";

import { isAlabamaCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { ALABAMA_DIRECT_COVERAGE_NOTE } from "./alabamaDirectFinanceAggregator.js";
import { isAlabamaFinanceEligibleOffice } from "./alabamaFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const ALABAMA_FCPA_SOURCE_URL = "https://fcpa.alabamavotes.gov/";
const ALABAMA_OUTSIDE_COVERAGE_NOTE =
  "Alabama disclosures do not identify independent expenditures by candidate or position, so outside spending totals are unavailable rather than zero.";

export async function loadAlabamaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "AL",
    source: "ALABAMA_FCPA",
    sourceUrl: ALABAMA_FCPA_SOURCE_URL,
    enabled: isAlabamaCampaignFinanceEnabled,
    isEligibleElection: (row) => isAlabamaFinanceEligibleOffice(officeInputFromElectionRow(row)),
    directCoverageNote: ALABAMA_DIRECT_COVERAGE_NOTE,
    outsideCoverageNote: ALABAMA_OUTSIDE_COVERAGE_NOTE,
    tables: {
      links: "al_candidate_finance_links",
      summaries: "al_candidate_finance_summaries",
      directBreakdowns: "al_candidate_finance_direct_breakdowns",
      outsideGroups: "al_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "al_candidate_finance_outside_group_breakdowns",
    },
  });
}
