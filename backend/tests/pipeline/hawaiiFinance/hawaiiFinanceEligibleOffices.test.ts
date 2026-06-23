import { describe, expect, it } from "vitest";

import {
  HAWAII_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isHawaiiFinanceEligibleOffice,
  mapHawaiiCscOffice,
  normalizeHawaiiCscDistrict,
  normalizeHawaiiCscOfficeLabel,
  toHawaiiCscOfficeSearchInput,
  toHawaiiFinanceOfficeKey,
} from "../../../src/pipeline/hawaiiFinance/hawaiiFinanceEligibleOffices.js";

describe("hawaiiFinanceEligibleOffices", () => {
  it("keeps a narrow explicit Hawaii finance office allowlist", () => {
    expect(HAWAII_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("allows every explicit eligible office and rejects broad-scope guesses", () => {
    for (const key of HAWAII_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isHawaiiFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }

    expect(isHawaiiFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Attorney General" })).toBe(false);
    expect(isHawaiiFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Secretary of State" })).toBe(false);
    expect(isHawaiiFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Prosecuting Attorney" })).toBe(false);
    expect(isHawaiiFinanceEligibleOffice({ officeScope: "city", officeCanonicalName: "Mayor" })).toBe(false);
    expect(isHawaiiFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Senator" })).toBe(false);
  });

  it("builds trimmed office keys and rejects missing parts", () => {
    expect(toHawaiiFinanceOfficeKey({ officeScope: " statewide ", officeCanonicalName: " Governor " })).toBe(
      "statewide::Governor"
    );
    expect(toHawaiiFinanceOfficeKey({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(toHawaiiFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: " " })).toBeNull();
  });

  it("normalizes Hawaii CSC office labels and legislative districts conservatively", () => {
    expect(normalizeHawaiiCscOfficeLabel(" Lt.   Governor ")).toBe("LT GOVERNOR");
    expect(normalizeHawaiiCscOfficeLabel("   ")).toBeNull();
    expect(normalizeHawaiiCscDistrict("1")).toBe("1");
    expect(normalizeHawaiiCscDistrict("01")).toBe("1");
    expect(normalizeHawaiiCscDistrict("District 12")).toBe("12");
    expect(normalizeHawaiiCscDistrict("15012")).toBeNull();
    expect(normalizeHawaiiCscDistrict("0")).toBeNull();
    expect(normalizeHawaiiCscDistrict("A")).toBeNull();
  });

  it("maps safe Hawaii CSC statewide office labels to canonical app offices", () => {
    expect(mapHawaiiCscOffice({ office: "Governor" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      officeKey: "statewide::Governor",
      cscOffice: "Governor",
      requiresDistrict: false,
      district: null,
    });
    expect(mapHawaiiCscOffice({ office: "Lt. Governor" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      officeKey: "statewide::Lieutenant Governor",
      cscOffice: "Lt. Governor",
      requiresDistrict: false,
      district: null,
    });
    expect(mapHawaiiCscOffice({ office: "Lieutenant Governor" })).toMatchObject({
      officeCanonicalName: "Lieutenant Governor",
      cscOffice: "Lt. Governor",
    });
  });

  it("requires districts for Hawaii CSC legislative offices", () => {
    expect(mapHawaiiCscOffice({ office: "Senate", district: "3" })).toEqual({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      officeKey: "state_upper::State Senator",
      cscOffice: "Senate",
      requiresDistrict: true,
      district: "3",
    });
    expect(mapHawaiiCscOffice({ office: "House", district: "04" })).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      officeKey: "state_lower::State Lower Chamber Legislator",
      cscOffice: "House",
      requiresDistrict: true,
      district: "4",
    });

    expect(mapHawaiiCscOffice({ office: "Senate" })).toBeNull();
    expect(mapHawaiiCscOffice({ office: "House", district: "   " })).toBeNull();
  });

  it("maps app canonical offices to Hawaii CSC search inputs", () => {
    expect(
      toHawaiiCscOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Governor",
      })
    ).toEqual({ cscOffice: "Governor", district: null });
    expect(
      toHawaiiCscOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Lieutenant Governor",
      })
    ).toEqual({ cscOffice: "Lt. Governor", district: null });
    expect(
      toHawaiiCscOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        district: "9",
      })
    ).toEqual({ cscOffice: "House", district: "9" });
  });

  it("rejects unsafe Hawaii CSC labels and incomplete app search inputs", () => {
    expect(mapHawaiiCscOffice({ office: "Mayor" })).toBeNull();
    expect(mapHawaiiCscOffice({ office: "Honolulu Council" })).toBeNull();
    expect(mapHawaiiCscOffice({ office: "Prosecuting Attorney" })).toBeNull();
    expect(mapHawaiiCscOffice({ office: "OHA" })).toBeNull();
    expect(mapHawaiiCscOffice({ office: "" })).toBeNull();

    expect(
      toHawaiiCscOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBeNull();
    expect(
      toHawaiiCscOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Attorney General",
      })
    ).toBeNull();
  });
});
