import { describe, expect, it } from "vitest";

import {
  isIllinoisFinanceEligibleOffice,
  mapIllinoisSbeOffice,
  toIllinoisSbeOfficeSearchInput,
} from "../../../src/pipeline/illinoisFinance/illinoisFinanceEligibleOffices.js";

describe("illinoisFinanceEligibleOffices", () => {
  it("recognizes supported Illinois statewide and legislative offices", () => {
    expect(isIllinoisFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(true);
    expect(
      isIllinoisFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(true);
    expect(isIllinoisFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBe(false);
  });

  it("maps app offices to SBE search labels and legislative districts", () => {
    expect(
      toIllinoisSbeOfficeSearchInput({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
        district: "SD 07",
      })
    ).toEqual({ sbeOffice: "State Senate", district: "7" });
    expect(
      toIllinoisSbeOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        district: "119",
      })
    ).toBeNull();
    expect(
      toIllinoisSbeOfficeSearchInput({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
        district: "HD 07",
      })
    ).toBeNull();
    expect(
      toIllinoisSbeOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        district: "Senate District 7",
      })
    ).toBeNull();
    expect(
      toIllinoisSbeOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Attorney General",
      })
    ).toEqual({ sbeOffice: "Attorney General", district: null });
  });

  it("maps SBE office labels back to app offices", () => {
    expect(mapIllinoisSbeOffice({ office: "State Representative", district: "House District 12" })).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      officeKey: "state_lower::State Lower Chamber Legislator",
      sbeOffice: "State Representative",
      requiresDistrict: true,
      district: "12",
      maxDistrict: 118,
    });
  });
});
