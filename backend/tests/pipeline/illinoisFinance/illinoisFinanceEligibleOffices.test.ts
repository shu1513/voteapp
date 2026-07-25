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
    expect(isIllinoisFinanceEligibleOffice({ officeScope: "place", officeCanonicalName: "Mayor" })).toBe(true);
    expect(isIllinoisFinanceEligibleOffice({ officeScope: "place", officeCanonicalName: "Municipal Trustee" })).toBe(true);
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
        officeScope: "statewide",
        officeCanonicalName: "State Treasurer",
      })
    ).toEqual({ sbeOffice: "Treasurer", district: null });
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

  it("maps only jurisdiction-safe local offices", () => {
    expect(mapIllinoisSbeOffice({ office: "Treasurer" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      officeKey: "statewide::State Treasurer",
    });
    expect(
      mapIllinoisSbeOffice({ office: "Treasurer", districtType: "City", district: "Aurora" })
    ).toMatchObject({
      officeScope: "place",
      officeCanonicalName: "City Treasurer",
      officeKey: "place::City Treasurer",
      district: "Aurora",
      sbeDistrictType: "City",
    });
    expect(
      mapIllinoisSbeOffice({ office: "President", districtType: "Village", district: "Oak Park" })
    ).toMatchObject({
      officeScope: "place",
      officeCanonicalName: "Mayor",
      officeKey: "place::Mayor",
      sbeOffice: "President",
      district: "Oak Park",
      sbeDistrictType: "Village",
    });
    expect(mapIllinoisSbeOffice({ office: "President", districtType: "City", district: "Chicago" })).toBeNull();
    expect(
      mapIllinoisSbeOffice({
        office: "Alderperson",
        districtType: "City",
        district: "Aurora",
        isAtLarge: true,
      })
    ).toMatchObject({
      officeScope: "place",
      officeCanonicalName: "Alderman",
      officeKey: "place::Alderman",
    });
    expect(
      mapIllinoisSbeOffice({ office: "Alderperson", districtType: "City", district: "Aurora" })
    ).toBeNull();
    expect(
      mapIllinoisSbeOffice({ office: "Alderperson", districtType: "Ward", district: "Chicago 44", isAtLarge: true })
    ).toBeNull();
  });

  it("builds exact local SBE search identities", () => {
    expect(
      toIllinoisSbeOfficeSearchInput({
        officeScope: "place",
        officeCanonicalName: "Mayor",
        districtType: "Village",
        district: "Villa Park",
        sbeOffice: "President",
      })
    ).toEqual({ sbeOffice: "President", district: "Villa Park", sbeDistrictType: "Village" });
    expect(
      toIllinoisSbeOfficeSearchInput({
        officeScope: "place",
        officeCanonicalName: "Municipal Trustee",
        districtType: "Village",
        district: "Schaumburg",
        isAtLarge: true,
      })
    ).toEqual({ sbeOffice: "Trustee", district: "Schaumburg", sbeDistrictType: "Village" });
  });
});
