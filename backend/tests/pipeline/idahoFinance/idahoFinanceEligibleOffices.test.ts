import { describe, expect, it } from "vitest";

import {
  IDAHO_FINANCE_ELIGIBLE_OFFICE_KEYS,
  idahoSunshineOfficeForRace,
  isIdahoFinanceEligibleOffice,
} from "../../../src/pipeline/idahoFinance/idahoFinanceEligibleOffices.js";

describe("idahoFinanceEligibleOffices", () => {
  it("maps VoteApp races to grid offices and district kinds", () => {
    expect(idahoSunshineOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Comptroller" })).toEqual({
      gridOffice: "State Controller",
      districtKind: "statewide",
    });
    expect(
      idahoSunshineOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })
    ).toEqual({ gridOffice: "State Representative", districtKind: "legislative" });
    expect(idahoSunshineOfficeForRace({ officeScope: "county", officeCanonicalName: "County Commissioner" })).toEqual({
      gridOffice: "County Commissioner",
      districtKind: "county_commissioner",
    });
    // Idaho's county clerk is the clerk of the district court ex officio.
    expect(idahoSunshineOfficeForRace({ officeScope: "county", officeCanonicalName: "Clerk of Court" })).toEqual({
      gridOffice: "Clerk",
      districtKind: "county",
    });
    expect(idahoSunshineOfficeForRace({ officeScope: "county", officeCanonicalName: "County Clerk" })?.gridOffice).toBe(
      "Clerk"
    );
  });

  it("fails closed outside the map", () => {
    expect(idahoSunshineOfficeForRace({ officeScope: "county", officeCanonicalName: "District Attorney" })).toBeNull();
    expect(idahoSunshineOfficeForRace({ officeScope: "us_house", officeCanonicalName: "United States Representative" })).toBeNull();
    expect(idahoSunshineOfficeForRace({ officeScope: null, officeCanonicalName: "Governor" })).toBeNull();
    expect(isIdahoFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(true);
    expect(isIdahoFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "United States Senator" })).toBe(
      false
    );
    expect(IDAHO_FINANCE_ELIGIBLE_OFFICE_KEYS.size).toBe(16);
  });
});
