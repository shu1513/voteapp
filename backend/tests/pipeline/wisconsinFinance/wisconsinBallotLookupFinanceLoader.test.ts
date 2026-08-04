import { afterEach, describe, vi } from "vitest";

import { loadWisconsinCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/wisconsinFinance/wisconsinBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("wisconsinBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadWisconsinCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "WISCONSIN_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "WI",
    tablePrefix: "wi_candidate_finance",
    source: "WISCONSIN_SUNSHINE",
    genericSourceUrl: "https://campaignfinance.wi.gov/",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
    ineligibleOffice: { office_scope: "place", office_canonical_name: "Mayor" },
  });
});
