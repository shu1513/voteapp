import { afterEach, describe, vi } from "vitest";

import { loadNorthCarolinaCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/northCarolinaFinance/northCarolinaBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("northCarolinaBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadNorthCarolinaCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "NORTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "NC",
    tablePrefix: "nc_candidate_finance",
    source: "NORTH_CAROLINA_SBE",
    genericSourceUrl: "https://cf.ncsbe.gov/CFOrgLkup/",
    eligibleOffice: { office_scope: "state_upper", office_canonical_name: "State Senator" },
    // NC 2026's only statewide election row is the US Senate race — federal
    // money belongs to the FEC, never this module (north_carolina_plan.md
    // decision 2).
    ineligibleOffice: { office_scope: "statewide", office_canonical_name: "United States Senator" },
    // north_carolina_plan.md decision 13: filings without a structured data
    // view (scanned images) stay out of the totals until a reviewed PDF/image
    // path ships. Pinned so the disclosure cannot quietly disappear while the
    // gap remains.
    outsideCoverageNote:
      "Covers outside spending from filings with structured data at the North Carolina State Board of Elections. " +
      "Filings available only as scanned images are not included yet.",
  });
});
