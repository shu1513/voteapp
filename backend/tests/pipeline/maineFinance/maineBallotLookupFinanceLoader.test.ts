import { afterEach, describe, vi } from "vitest";

import { loadMaineCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/maineFinance/maineBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("maineBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadMaineCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "MAINE_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "ME",
    tablePrefix: "me_candidate_finance",
    source: "MAINE_CFIS",
    genericSourceUrl: "https://mainecampaignfinance.com/",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
    ineligibleOffice: { office_scope: "place", office_canonical_name: "Mayor" },
  });
});
