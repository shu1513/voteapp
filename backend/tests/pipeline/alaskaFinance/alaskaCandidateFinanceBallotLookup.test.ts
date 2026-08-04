import { afterEach, describe, vi } from "vitest";

import { loadAlaskaCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/alaskaFinance/alaskaCandidateFinanceBallotLookup.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("alaskaCandidateFinanceBallotLookup", () => {
  runStateFinanceLoaderCharacterization({
    load: loadAlaskaCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "ALASKA_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "AK",
    tablePrefix: "ak_candidate_finance",
    source: "ALASKA_APOC",
    genericSourceUrl: "https://aws.state.ak.us/ApocReports/Campaign/",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
    ineligibleOffice: { office_scope: "place", office_canonical_name: "Mayor" },
  });
});
