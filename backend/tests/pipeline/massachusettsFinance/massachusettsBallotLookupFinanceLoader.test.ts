import { afterEach, describe, vi } from "vitest";

import { loadMassachusettsCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/massachusettsFinance/massachusettsBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("massachusettsBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadMassachusettsCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "MASSACHUSETTS_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "MA",
    tablePrefix: "ma_candidate_finance",
    source: "MASSACHUSETTS_OCPF",
    genericSourceUrl: "https://www.ocpf.us/",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
    ineligibleOffice: { office_scope: "place", office_canonical_name: "Mayor" },
  });
});
