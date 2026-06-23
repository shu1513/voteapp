import { describe, expect, it } from "vitest";

import {
  WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isWashingtonFinanceEligibleOffice,
  mapWashingtonPdcOffice,
  normalizeWashingtonPdcLegislativeDistrict,
  normalizeWashingtonPdcOfficeLabel,
  toWashingtonFinanceOfficeKey,
  toWashingtonPdcOfficeSearchInput,
} from "../../../src/pipeline/washingtonFinance/washingtonFinanceEligibleOffices.js";

describe("washingtonFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Washington finance office allowlist", () => {
    expect(WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Secretary of State",
      "statewide::Attorney General",
      "statewide::State Treasurer",
      "statewide::State Auditor",
      "statewide::Land Commissioner",
      "statewide::Commissioner of Insurance",
      "statewide::Superintendent of Public Instruction",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("allows every explicit eligible office and rejects broad-scope guesses", () => {
    for (const key of WASHINGTON_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isWashingtonFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }

    expect(
      isWashingtonFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Public Service Commissioner",
      })
    ).toBe(false);
    expect(
      isWashingtonFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Supreme Court Justice",
      })
    ).toBe(false);
    expect(
      isWashingtonFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "County Commissioner",
      })
    ).toBe(false);
    expect(
      isWashingtonFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });

  it("builds trimmed office keys and rejects missing parts", () => {
    expect(
      toWashingtonFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
    expect(toWashingtonFinanceOfficeKey({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(toWashingtonFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: " " })).toBeNull();
  });

  it("normalizes PDC office labels and legislative districts conservatively", () => {
    expect(normalizeWashingtonPdcOfficeLabel(" public   lands commissioner ")).toBe("PUBLIC LANDS COMMISSIONER");
    expect(normalizeWashingtonPdcOfficeLabel("   ")).toBeNull();
    expect(normalizeWashingtonPdcLegislativeDistrict("1")).toBe("01");
    expect(normalizeWashingtonPdcLegislativeDistrict("01")).toBe("01");
    expect(normalizeWashingtonPdcLegislativeDistrict("LD 7")).toBe("07");
    expect(normalizeWashingtonPdcLegislativeDistrict("Leg District 12")).toBe("12");
    expect(normalizeWashingtonPdcLegislativeDistrict("53012")).toBeNull();
    expect(normalizeWashingtonPdcLegislativeDistrict("0")).toBeNull();
  });

  it("maps safe PDC statewide office labels to canonical app offices", () => {
    expect(mapWashingtonPdcOffice({ office: "PUBLIC LANDS COMMISSIONER" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Land Commissioner",
      officeKey: "statewide::Land Commissioner",
      pdcOffice: "PUBLIC LANDS COMMISSIONER",
      requiresLegislativeDistrict: false,
      legislativeDistrict: null,
    });
    expect(mapWashingtonPdcOffice({ office: "INSURANCE COMMISSIONER" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Commissioner of Insurance",
      pdcOffice: "INSURANCE COMMISSIONER",
    });
    expect(mapWashingtonPdcOffice({ office: "SUPERINTENDENT OF PUBLIC INSTRUCTION" })).toMatchObject({
      officeCanonicalName: "Superintendent of Public Instruction",
    });
  });

  it("requires districts for PDC legislative offices", () => {
    expect(mapWashingtonPdcOffice({ office: "STATE SENATOR", legislativeDistrict: "3" })).toEqual({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      officeKey: "state_upper::State Senator",
      pdcOffice: "STATE SENATOR",
      requiresLegislativeDistrict: true,
      legislativeDistrict: "03",
    });
    expect(mapWashingtonPdcOffice({ office: "STATE REPRESENTATIVE", legislativeDistrict: "04" })).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      officeKey: "state_lower::State Lower Chamber Legislator",
      pdcOffice: "STATE REPRESENTATIVE",
      requiresLegislativeDistrict: true,
      legislativeDistrict: "04",
    });

    expect(mapWashingtonPdcOffice({ office: "STATE SENATOR" })).toBeNull();
    expect(mapWashingtonPdcOffice({ office: "STATE REPRESENTATIVE", legislativeDistrict: "   " })).toBeNull();
  });

  it("maps app canonical offices to PDC search inputs", () => {
    expect(
      toWashingtonPdcOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Attorney General",
      })
    ).toEqual({ pdcOffice: "ATTORNEY GENERAL", legislativeDistrict: null });
    expect(
      toWashingtonPdcOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Land Commissioner",
      })
    ).toEqual({ pdcOffice: "PUBLIC LANDS COMMISSIONER", legislativeDistrict: null });
    expect(
      toWashingtonPdcOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        legislativeDistrict: "9",
      })
    ).toEqual({ pdcOffice: "STATE REPRESENTATIVE", legislativeDistrict: "09" });
  });

  it("rejects unsafe PDC labels and incomplete app search inputs", () => {
    expect(mapWashingtonPdcOffice({ office: "PUBLIC UTILITY COMMISSIONER" })).toBeNull();
    expect(mapWashingtonPdcOffice({ office: "STATE SUPREME COURT JUSTICE" })).toBeNull();
    expect(mapWashingtonPdcOffice({ office: "COUNTY SHERIFF" })).toBeNull();
    expect(mapWashingtonPdcOffice({ office: "" })).toBeNull();

    expect(
      toWashingtonPdcOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBeNull();
    expect(
      toWashingtonPdcOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "State Supreme Court Justice",
      })
    ).toBeNull();
  });
});
