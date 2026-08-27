import type { Pool, PoolClient } from "pg";

import { isNevadaCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  officeInputFromElectionRow,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { isNevadaFinanceEligibleOffice } from "./nevadaFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

const NEVADA_AURORA_SOURCE_URL =
  "https://www.nvsos.gov/SOSCandidateServices/AnonymousAccess/CEFDSearchUU/Search.aspx";
const NEVADA_DIRECT_COVERAGE_NOTE =
  "Nevada does not collect donor occupation or employer; industry breakdowns cover identifiable organization donors only, and contribution-size buckets cover itemized transactions only. Totals cover the election year plus the prior year from searchable electronic filings.";
const NEVADA_OUTSIDE_COVERAGE_NOTE =
  "Candidate target and support/oppose direction are not available in Nevada SOS report data, so outside-spending totals are not shown for Nevada state races.";

export async function loadNevadaCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "NV",
    source: "NEVADA_AURORA",
    sourceUrl: NEVADA_AURORA_SOURCE_URL,
    enabled: isNevadaCampaignFinanceEnabled,
    isEligibleElection: (row) => isNevadaFinanceEligibleOffice(officeInputFromElectionRow(row)),
    linkIdentityColumn: "filer_key",
    outsideGroupIdentityColumns: {
      id: "filer_key",
      name: "filer_name",
    },
    directBreakdownCategoryTypes: ["industry", "contribution_size"],
    evidenceLabelTypes: ["donor"],
    directCoverageNote: NEVADA_DIRECT_COVERAGE_NOTE,
    outsideCoverageNote: NEVADA_OUTSIDE_COVERAGE_NOTE,
    tables: {
      links: "nv_candidate_finance_links",
      summaries: "nv_candidate_finance_summaries",
      directBreakdowns: "nv_candidate_finance_direct_breakdowns",
      outsideGroups: "nv_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "nv_candidate_finance_outside_group_breakdowns",
    },
  });
}
