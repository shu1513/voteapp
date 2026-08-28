import { describe, expect, it } from "vitest";

import { isNevadaFinanceEligibleOffice } from "../../../src/pipeline/nevadaFinance/nevadaFinanceEligibleOffices.js";

describe("isNevadaFinanceEligibleOffice", () => {
  it("accepts NV SOS-jurisdiction offices and rejects everything else", () => {
    expect(
      isNevadaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })
    ).toBe(true);
    expect(
      isNevadaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Comptroller" })
    ).toBe(true);
    expect(
      isNevadaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(true);
    expect(
      isNevadaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "State Level Judge" })
    ).toBe(true);
    // County/city/school/US House filers are out of scope for v1.
    expect(isNevadaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBe(
      false
    );
    expect(
      isNevadaFinanceEligibleOffice({
        officeScope: "us_house",
        officeCanonicalName: "United States Representative",
      })
    ).toBe(false);
    expect(isNevadaFinanceEligibleOffice({ officeScope: null, officeCanonicalName: "Governor" })).toBe(
      false
    );
  });
});
