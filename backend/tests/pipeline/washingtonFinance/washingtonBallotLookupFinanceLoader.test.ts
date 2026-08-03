import { afterEach, describe, vi } from "vitest";

import { loadWashingtonCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/washingtonFinance/washingtonBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("washingtonBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadWashingtonCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "WASHINGTON_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "WA",
    tablePrefix: "wa_candidate_finance",
    source: "WASHINGTON_PDC",
    genericSourceUrl: "https://www.pdc.wa.gov/political-disclosure-reporting-data/browse-search-data",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
  });
});
