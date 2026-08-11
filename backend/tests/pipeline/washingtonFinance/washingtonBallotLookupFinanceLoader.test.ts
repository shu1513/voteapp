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
    // Washington discloses occupation only for donors above $250 aggregate,
    // and batched small contributions (incl. Seattle democracy vouchers) carry
    // no donor detail. Pinned so the disclosure cannot quietly disappear.
    directCoverageNote:
      "Occupation breakdowns reflect donors whose contributions exceed $250 in aggregate, the threshold above " +
      "which Washington requires occupation disclosure. Smaller and batched contributions are included in the " +
      "official totals but carry no donor detail, and campaigns using Washington's mini-reporting option file " +
      "no itemized contribution reports at all.",
  });
});
