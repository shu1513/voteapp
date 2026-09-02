import { describe, expect, it } from "vitest";

import {
  ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS,
  arkansasCfisOfficeNameForOffice,
  isArkansasFinanceEligibleOffice,
} from "../../../src/pipeline/arkansasFinance/arkansasFinanceEligibleOffices.js";

describe("arkansasFinanceEligibleOffices", () => {
  it("maps every eligible VoteApp office to a pinned CFIS office name", () => {
    for (const key of ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isArkansasFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
      expect(arkansasCfisOfficeNameForOffice({ officeScope, officeCanonicalName })).toEqual(expect.any(String));
    }
    expect(
      arkansasCfisOfficeNameForOffice({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })
    ).toBe("State Representative");
    expect(arkansasCfisOfficeNameForOffice({ officeScope: "statewide", officeCanonicalName: "State Auditor" })).toBe(
      "Auditor Of State"
    );
  });

  it("fails closed for federal, local, judicial, and blank offices", () => {
    expect(isArkansasFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "United States Senator" })).toBe(false);
    expect(arkansasCfisOfficeNameForOffice({ officeScope: "county", officeCanonicalName: "Justice of the Peace" })).toBeNull();
    expect(arkansasCfisOfficeNameForOffice({ officeScope: "place", officeCanonicalName: "Mayor" })).toBeNull();
    expect(arkansasCfisOfficeNameForOffice({ officeScope: "statewide", officeCanonicalName: "State Level Judge" })).toBeNull();
    expect(arkansasCfisOfficeNameForOffice({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(arkansasCfisOfficeNameForOffice({ officeScope: "statewide", officeCanonicalName: null })).toBeNull();
  });
});
