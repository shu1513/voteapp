import { describe, expect, it } from "vitest";

import {
  MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isMichiganFinanceEligibleOffice,
  mapMichiganMitnOffice,
  normalizeMichiganMitnLegislativeDistrict,
  normalizeMichiganMitnOfficeLabel,
  toMichiganFinanceOfficeKey,
  toMichiganMitnOfficeSearchInput,
} from "../../../src/pipeline/michiganFinance/michiganFinanceEligibleOffices.js";

describe("michiganFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Michigan finance office allowlist", () => {
    expect(MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Secretary of State",
      "statewide::Attorney General",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("allows every explicit eligible office and rejects broad-scope guesses", () => {
    for (const key of MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isMichiganFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }

    expect(
      isMichiganFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Treasurer",
      })
    ).toBe(false);
    expect(
      isMichiganFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Superintendent of Public Instruction",
      })
    ).toBe(false);
    expect(
      isMichiganFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Supreme Court Justice",
      })
    ).toBe(false);
    expect(
      isMichiganFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "University Regent",
      })
    ).toBe(false);
    expect(
      isMichiganFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
    expect(
      isMichiganFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });

  it("builds trimmed office keys and rejects missing parts", () => {
    expect(
      toMichiganFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
    expect(toMichiganFinanceOfficeKey({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(toMichiganFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: " " })).toBeNull();
  });

  it("normalizes Michigan MiTN office labels and legislative districts conservatively", () => {
    expect(normalizeMichiganMitnOfficeLabel(" state   representative. ")).toBe("STATE REPRESENTATIVE");
    expect(normalizeMichiganMitnOfficeLabel("   ")).toBeNull();
    expect(normalizeMichiganMitnLegislativeDistrict("1", 110)).toBe("1");
    expect(normalizeMichiganMitnLegislativeDistrict("001", 110)).toBe("1");
    expect(normalizeMichiganMitnLegislativeDistrict("HD 7", 110)).toBe("7");
    expect(normalizeMichiganMitnLegislativeDistrict("Senate District 12", 38)).toBe("12");
    expect(normalizeMichiganMitnLegislativeDistrict("Leg District 110", 110)).toBe("110");
    expect(normalizeMichiganMitnLegislativeDistrict("39", 38)).toBeNull();
    expect(normalizeMichiganMitnLegislativeDistrict("111", 110)).toBeNull();
    expect(normalizeMichiganMitnLegislativeDistrict("55012", 110)).toBeNull();
    expect(normalizeMichiganMitnLegislativeDistrict("0", 110)).toBeNull();
    expect(normalizeMichiganMitnLegislativeDistrict("A", 110)).toBeNull();
  });

  it("maps safe Michigan office labels to canonical app offices", () => {
    expect(mapMichiganMitnOffice({ office: "Governor" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      officeKey: "statewide::Governor",
      mitnOffice: "Governor",
      requiresDistrict: false,
      maxDistrict: null,
      district: null,
    });
    expect(mapMichiganMitnOffice({ office: "Lt. Governor" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Lieutenant Governor",
      mitnOffice: "Lieutenant Governor",
    });
    expect(mapMichiganMitnOffice({ office: "Secretary of State" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
    });
    expect(mapMichiganMitnOffice({ office: "Attorney General" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
    });
  });

  it("requires valid districts for Michigan legislative offices", () => {
    expect(mapMichiganMitnOffice({ office: "State Senate", district: "SD 04" })).toEqual({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      officeKey: "state_upper::State Senator",
      mitnOffice: "State Senate",
      requiresDistrict: true,
      maxDistrict: 38,
      district: "4",
    });
    expect(mapMichiganMitnOffice({ office: "State Representative", district: "House District 103" })).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      officeKey: "state_lower::State Lower Chamber Legislator",
      mitnOffice: "State House",
      requiresDistrict: true,
      maxDistrict: 110,
      district: "103",
    });

    expect(mapMichiganMitnOffice({ office: "State Senate" })).toBeNull();
    expect(mapMichiganMitnOffice({ office: "State Senate", district: "39" })).toBeNull();
    expect(mapMichiganMitnOffice({ office: "State Representative", district: "111" })).toBeNull();
  });

  it("maps app canonical offices to Michigan MiTN search inputs", () => {
    expect(
      toMichiganMitnOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Attorney General",
      })
    ).toEqual({ mitnOffice: "Attorney General", district: null });
    expect(
      toMichiganMitnOfficeSearchInput({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
        district: "Senate District 8",
      })
    ).toEqual({ mitnOffice: "State Senate", district: "8" });
    expect(
      toMichiganMitnOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        district: "HD 22",
      })
    ).toEqual({ mitnOffice: "State House", district: "22" });
  });

  it("rejects unsafe office labels and incomplete app search inputs", () => {
    expect(mapMichiganMitnOffice({ office: "State Treasurer" })).toBeNull();
    expect(mapMichiganMitnOffice({ office: "Supreme Court" })).toBeNull();
    expect(mapMichiganMitnOffice({ office: "University Regent" })).toBeNull();
    expect(mapMichiganMitnOffice({ office: "County Commissioner" })).toBeNull();
    expect(mapMichiganMitnOffice({ office: "" })).toBeNull();

    expect(
      toMichiganMitnOfficeSearchInput({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
      })
    ).toBeNull();
    expect(
      toMichiganMitnOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "State Treasurer",
      })
    ).toBeNull();
  });
});
