import { afterEach, describe, vi } from "vitest";

import { loadMissouriCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/missouriFinance/missouriBallotLookupFinanceLoader.js";
import { runStateFinanceLoaderCharacterization } from "../../helpers/stateFinanceLoaderCharacterization.js";

afterEach(() => vi.unstubAllEnvs());

describe("missouriBallotLookupFinanceLoader", () => {
  runStateFinanceLoaderCharacterization({
    load: loadMissouriCandidateFinanceSummariesByCandidateElection,
    flagEnvVar: "MISSOURI_CAMPAIGN_FINANCE_ENABLED",
    stateCode: "MO",
    tablePrefix: "mo_candidate_finance",
    source: "MISSOURI_MEC",
    genericSourceUrl: "https://www.mec.mo.gov/MEC/Campaign_Finance/",
    eligibleOffice: { office_scope: "state_lower", office_canonical_name: "State Lower Chamber Legislator" },
    ineligibleOffice: { office_scope: "statewide", office_canonical_name: "United States Senator" },
    directCoverageNote:
      "Totals and donor breakdowns are summed from itemized Missouri Ethics Commission filings and are not reconciled to official report covers.",
    outsideCoverageNote:
      "Registered-committee reported spending only; Missouri non-committee expenditure reports (§ 130.047) are not included.",
    outsideSupportActionLabel:
      "independent spending supporting this candidate; listed contributions are committee-cycle funding, not money earmarked to that spending",
  });
});
