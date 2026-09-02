// Arkansas ballot-lookup finance loader: thin config wrapper over the
// standard loader. Outside totals are never stored (Arkansas publishes no
// structured independent-expenditure target or stance — Phase 0 finding), so
// they surface as null with the coverage note, never $0. Identity columns
// follow migration 266 (filing_entity_id / filer_name, the NH shape).

import type { Pool, PoolClient } from "pg";

import { isArkansasCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { ARKANSAS_CFIS_PUBLIC_URL } from "./arkansasCfisClient.js";
import { isArkansasFinanceEligibleOffice } from "./arkansasFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Totals are the state system's own registration figures; breakdowns are
// computed from itemized receipts and published only when those receipts
// reconcile to the state total (arkansasDirectContributionAggregator.ts).
export const ARKANSAS_DIRECT_COVERAGE_NOTE =
  "Totals are the Arkansas Secretary of State's registration figures; occupation and contribution-size breakdowns are computed from itemized receipts, so non-itemized contributions are not broken down.";
export const ARKANSAS_OUTSIDE_COVERAGE_NOTE =
  "Arkansas disclosures do not identify independent expenditures by candidate or position, so outside spending totals are unavailable rather than zero.";

export async function loadArkansasCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "AR",
    source: "ARKANSAS_CFIS",
    sourceUrl: ARKANSAS_CFIS_PUBLIC_URL,
    enabled: isArkansasCampaignFinanceEnabled,
    isEligibleElection: (row) => isArkansasFinanceEligibleOffice(officeInputFromElectionRow(row)),
    linkIdentityColumn: "filing_entity_id",
    outsideGroupIdentityColumns: {
      id: "filing_entity_id",
      name: "filer_name",
    },
    directCoverageNote: ARKANSAS_DIRECT_COVERAGE_NOTE,
    outsideCoverageNote: ARKANSAS_OUTSIDE_COVERAGE_NOTE,
    tables: {
      links: "ar_candidate_finance_links",
      summaries: "ar_candidate_finance_summaries",
      directBreakdowns: "ar_candidate_finance_direct_breakdowns",
      outsideGroups: "ar_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "ar_candidate_finance_outside_group_breakdowns",
    },
  });
}
