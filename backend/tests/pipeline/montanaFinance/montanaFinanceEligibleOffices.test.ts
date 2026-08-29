import { describe, expect, it } from "vitest";

import {
  isMontanaFinanceEligibleOffice,
  MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS,
} from "../../../src/pipeline/montanaFinance/montanaFinanceEligibleOffices.js";

describe("montanaFinanceEligibleOffices", () => {
  it("covers the Phase 3 scope: statewide judicial + PSC + both chambers", () => {
    expect([...MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS].sort()).toEqual([
      "state_lower::State Lower Chamber Legislator",
      "state_upper::State Senator",
      "statewide::Public Service Commissioner",
      "statewide::State Level Judge",
    ]);
  });

  it("gates eligibility on scope + canonical name", () => {
    expect(
      isMontanaFinanceEligibleOffice({ officeScope: "state_upper", officeCanonicalName: "State Senator" })
    ).toBe(true);
    // County and local offices stay behind a second validated pass; federal
    // races are the FEC path.
    expect(isMontanaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBe(false);
    expect(
      isMontanaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "United States Senator" })
    ).toBe(false);
    expect(isMontanaFinanceEligibleOffice({ officeScope: null, officeCanonicalName: null })).toBe(false);
  });
});
