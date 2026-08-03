import { afterEach, describe, vi } from "vitest";

import { loadHawaiiCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/hawaiiFinance/hawaiiBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("hawaiiBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadHawaiiCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "HAWAII_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "HI",
    tablePrefix: "hi_candidate_finance",
    source: "HAWAII_CSC",
    genericSourceUrl: "https://hicscdata.hawaii.gov/",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
  });
});
