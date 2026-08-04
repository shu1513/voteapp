import { afterEach, describe, vi } from "vitest";

import { loadOhioCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/ohioFinance/ohioBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("ohioBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadOhioCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "OHIO_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "OH",
    tablePrefix: "oh_candidate_finance",
    source: "OHIO_SOS",
    genericSourceUrl: "https://www.ohiosos.gov/campaign-finance/search/",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
    // Ohio elects the Lieutenant Governor on a joint ticket with the Governor,
    // so no separate election rows exist and the office is deliberately absent
    // from the eligible-office allowlist.
    ineligibleOffice: { office_scope: "statewide", office_canonical_name: "Lieutenant Governor" },
  });
});
