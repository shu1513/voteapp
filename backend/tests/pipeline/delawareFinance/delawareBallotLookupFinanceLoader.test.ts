import { afterEach, describe, vi } from "vitest";

import { loadDelawareCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/delawareFinance/delawareBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => vi.unstubAllEnvs());

describe("delawareBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadDelawareCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "DELAWARE_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "DE",
    tablePrefix: "de_candidate_finance",
    source: "DELAWARE_CFRS",
    genericSourceUrl: "https://cfrs.elections.delaware.gov/",
    eligibleOffice: { office_scope: "statewide", office_canonical_name: "Attorney General" },
    ineligibleOffice: { office_scope: "county", office_canonical_name: "Sheriff" },
    directCoverageNote:
      "Delaware does not require donor occupation disclosure; occupation charts reflect voluntarily disclosed contributions only.",
    outsideCoverageNote:
      "Delaware filings do not link each outside expenditure to a candidate and position, so outside spending totals are unavailable rather than zero.",
  });
});
