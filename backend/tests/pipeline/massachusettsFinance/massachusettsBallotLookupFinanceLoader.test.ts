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
    // Outside leg reads the IEPAC feed only — ordinary IE filers and
    // electioneering communications are not read yet. Pinned so the
    // disclosure cannot quietly disappear while the gap remains.
    outsideCoverageNote:
      "Covers independent expenditures reported by independent expenditure PACs (IEPACs) to the Massachusetts " +
      "Office of Campaign and Political Finance. Independent spending by other filer types and electioneering " +
      "communications are not included yet.",
    // Official totals are bank-report YTD cover figures where available; the
    // no-YTD-row fallback stores the itemized-receipt sum, so the note says
    // "where available" rather than promising a bank figure for everyone.
    // Pinned so the disclosure cannot quietly disappear.
    directCoverageNote:
      "Donor breakdowns reflect itemized receipts reported to the Massachusetts Office of Campaign and Political " +
      "Finance. Totals are official bank-report year-to-date figures where available (otherwise sums of itemized " +
      "receipts) and can include money not shown in the breakdowns.",
  });
});
