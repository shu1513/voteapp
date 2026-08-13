// Ballot-lookup read side for Denver (plan Phase 3): a thin config wrapper
// over the shared standard-shape loader. Denver deltas: link identity is
// filer_id (not committee_id), outside tables key on spender_id/spender_name
// (the resolved SearchLight "Ind…" uniqueId), and the Fair Elections Fund
// disclosure rides the per-source directCoverageNote.
//
// COLORADO_TRACER coexistence: the Colorado state loader also registers for
// CO, but Denver municipal filers are not in TRACER, so no co_ link rows can
// exist for a Denver city candidate — the two sources can never both return
// a summary for one candidate/election, and no Texas/Houston-style office
// exclusion is needed in the Colorado loader.

import type { Pool, PoolClient } from "pg";

import { isDenverCampaignFinanceEnabled } from "../../config/featureFlags.js";
import type {
  BallotLookupFinanceSummary,
  StateFinanceRequestCandidateRow,
  StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { loadStandardStateFinanceSummariesByCandidateElection } from "../finance/standardStateFinanceBallotLookupLoader.js";
import { DENVER_FINANCE_SOURCE_URL } from "./denverCandidateFinanceAutoLink.js";
import {
  DENVER_CITY_GEOID,
  DENVER_FINANCE_ELIGIBLE_OFFICE_NAMES,
} from "./denverFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// The card's raised figure is direct_contribution_total — private donor money
// only (the standard loader prefers it over total_receipts). Fair Elections
// Fund public matching and candidate loans are real campaign money the
// committee spends, so without this sentence "spent" visibly exceeding
// "raised" reads as an error.
const DENVER_DIRECT_COVERAGE_NOTE =
  "Raised counts private contributions to the candidate's committee only; candidate loans and public " +
  "matching from Denver's Fair Elections Fund are not included in the raised figure but are available " +
  "to the campaign, so spending can exceed the amount raised.";

export async function loadDenverCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[],
): Promise<Map<string, BallotLookupFinanceSummary>> {
  return loadStandardStateFinanceSummariesByCandidateElection({
    db,
    candidateRows,
    electionRows,
    state: "CO",
    source: "DENVER_CLERK_RECORDER",
    sourceUrl: DENVER_FINANCE_SOURCE_URL,
    enabled: isDenverCampaignFinanceEnabled,
    // Structural gate only: election rows carry no ballot title, so the
    // at-large seat-letter rule cannot run here. Denver-linked rows exist
    // only for candidates the (fully gated) sync wrote, so this is a
    // narrowing filter, not the eligibility authority.
    isEligibleElection: (row) =>
      row.district_type === "place" &&
      row.geoid_compact === DENVER_CITY_GEOID &&
      row.office_scope === "place" &&
      (DENVER_FINANCE_ELIGIBLE_OFFICE_NAMES as readonly string[]).includes(
        row.office_canonical_name ?? "",
      ),
    linkIdentityColumn: "filer_id",
    outsideGroupIdentityColumns: { id: "spender_id", name: "spender_name" },
    directCoverageNote: DENVER_DIRECT_COVERAGE_NOTE,
    tables: {
      links: "denver_candidate_finance_links",
      summaries: "denver_candidate_finance_summaries",
      directBreakdowns: "denver_candidate_finance_direct_breakdowns",
      outsideGroups: "denver_candidate_finance_outside_groups",
      outsideGroupBreakdowns: "denver_candidate_finance_outside_group_breakdowns",
    },
  });
}
