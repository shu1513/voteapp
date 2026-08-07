import { afterEach, describe, vi } from "vitest";

import { loadGeorgiaCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/georgiaFinance/georgiaBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("georgiaBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadGeorgiaCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "GEORGIA_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "GA",
    tablePrefix: "ga_candidate_finance",
    source: "GEORGIA_ETHICS",
    genericSourceUrl: "https://ethics.ga.gov/records-search-all/",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Governor" },
    // United States Senator is GA 2026's only non-state statewide election
    // row — federal money belongs to the FEC, never this module
    // (georgia_plan.md D9).
    ineligibleOffice: { office_scope: "statewide", office_canonical_name: "United States Senator" },
    // georgia_plan.md D6/D12: no per-target amount exists on Georgia
    // independent expenditures, so any multi-target expenditure — several
    // candidates, or a candidate plus a ballot measure — stays out of the
    // per-candidate totals. Pinned so the disclosure cannot quietly
    // disappear while the gap remains.
    outsideCoverageNote:
      "Covers independent expenditures that name a single candidate, as reported to the Georgia Government " +
      "Transparency and Campaign Finance Commission. Spending reported for more than one candidate or measure " +
      "in a single expenditure is not included yet.",
  });
});
