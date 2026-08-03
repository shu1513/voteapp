import { afterEach, describe, vi } from "vitest";

import { loadArizonaCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/arizonaFinance/arizonaFinanceBallotLookup.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("arizonaFinanceBallotLookup", () => {
  runStateFinanceLoaderCharacterization({
    load: loadArizonaCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "ARIZONA_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "AZ",
    tablePrefix: "az_candidate_finance",
    source: "ARIZONA_SOS",
    genericSourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
  });
});
