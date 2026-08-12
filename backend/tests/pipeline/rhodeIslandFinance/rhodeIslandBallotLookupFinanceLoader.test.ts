import { afterEach, describe, vi } from "vitest";

import { loadRhodeIslandCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("rhodeIslandBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadRhodeIslandCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "RHODE_ISLAND_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "RI",
    tablePrefix: "ri_candidate_finance",
    source: "RHODE_ISLAND_ERTS",
    genericSourceUrl: "https://www.ricampaignfinance.com/RIPublic/Homepage.aspx",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
    // Federal money belongs to the FEC, never this module — same boundary
    // every state pins (georgia precedent; also RI PR 1's eligible-offices
    // test).
    ineligibleOffice: { office_scope: "statewide", office_canonical_name: "United States Senator" },
    // rhode_island_plan.md decision 1: RI discloses employer, never
    // occupation, so the loader selects contribution-size buckets only
    // (Louisiana/Vermont narrowing).
    directCategoryTypes: ["contribution_size"],
    // Decisions 5/7 (curated scanned CF-8s, required-apportionment
    // quarantine, $1,000 donor-disclosure floor) and 1/13 (no occupations,
    // Aggregate-* in totals but not buckets). Pinned so the disclosures
    // cannot quietly disappear while the gaps remain.
    outsideCoverageNote:
      "Outside-spending filings in Rhode Island are scanned documents; totals include manually verified filings " +
      "with a clear per-candidate amount — filings naming several candidates without a stated split are excluded — " +
      "and the state requires spenders to disclose only donors above $1,000 per cycle, with statutory exceptions.",
    directCoverageNote:
      "Rhode Island discloses a direct contributor's employer, not occupation, so donor-occupation breakdowns " +
      "are not available for this state; size buckets reflect itemized contributions only.",
  });
});
