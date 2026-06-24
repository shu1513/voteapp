import { describe, expect, it } from "vitest";

import {
  WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isWisconsinFinanceEligibleOffice,
  mapWisconsinSunshineOffice,
  normalizeWisconsinSunshineLegislativeDistrict,
  normalizeWisconsinSunshineOfficeLabel,
  toWisconsinFinanceOfficeKey,
  toWisconsinSunshineOfficeSearchInput,
} from "../../../src/pipeline/wisconsinFinance/wisconsinFinanceEligibleOffices.js";

describe("wisconsinFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Wisconsin finance office allowlist", () => {
    expect(WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Secretary of State",
      "statewide::Attorney General",
      "statewide::State Treasurer",
      "statewide::Superintendent of Public Instruction",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("allows every explicit eligible office and rejects broad-scope guesses", () => {
    for (const key of WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isWisconsinFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }

    expect(
      isWisconsinFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Commissioner of Insurance",
      })
    ).toBe(false);
    expect(
      isWisconsinFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Supreme Court Justice",
      })
    ).toBe(false);
    expect(
      isWisconsinFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "District Attorney",
      })
    ).toBe(false);
    expect(
      isWisconsinFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });

  it("builds trimmed office keys and rejects missing parts", () => {
    expect(
      toWisconsinFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
    expect(toWisconsinFinanceOfficeKey({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(toWisconsinFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: " " })).toBeNull();
  });

  it("normalizes Wisconsin Sunshine office labels and legislative districts conservatively", () => {
    expect(normalizeWisconsinSunshineOfficeLabel(" state   assembly ")).toBe("STATE ASSEMBLY");
    expect(normalizeWisconsinSunshineOfficeLabel("   ")).toBeNull();
    expect(normalizeWisconsinSunshineLegislativeDistrict("1")).toBe("1");
    expect(normalizeWisconsinSunshineLegislativeDistrict("01")).toBe("1");
    expect(normalizeWisconsinSunshineLegislativeDistrict("AD 7")).toBe("7");
    expect(normalizeWisconsinSunshineLegislativeDistrict("Senate District 12")).toBe("12");
    expect(normalizeWisconsinSunshineLegislativeDistrict("Leg District 99")).toBe("99");
    expect(normalizeWisconsinSunshineLegislativeDistrict("55012")).toBeNull();
    expect(normalizeWisconsinSunshineLegislativeDistrict("0")).toBeNull();
    expect(normalizeWisconsinSunshineLegislativeDistrict("A")).toBeNull();
  });

  it("maps safe Wisconsin Sunshine statewide office labels to canonical app offices", () => {
    expect(mapWisconsinSunshineOffice({ office: "Governor" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      officeKey: "statewide::Governor",
      sunshineOffice: "Governor",
      requiresDistrict: false,
      district: null,
    });
    expect(mapWisconsinSunshineOffice({ office: "Secretary of State" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      sunshineOffice: "Secretary of State",
    });
    expect(mapWisconsinSunshineOffice({ office: "Superintendent of Public Instruction" })).toMatchObject({
      officeCanonicalName: "Superintendent of Public Instruction",
    });
  });

  it("requires districts for Wisconsin Sunshine legislative offices", () => {
    expect(mapWisconsinSunshineOffice({ office: "State Senate", district: "3" })).toEqual({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      officeKey: "state_upper::State Senator",
      sunshineOffice: "State Senate",
      requiresDistrict: true,
      district: "3",
    });
    expect(mapWisconsinSunshineOffice({ office: "State Assembly", district: "AD 04" })).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      officeKey: "state_lower::State Lower Chamber Legislator",
      sunshineOffice: "State Assembly",
      requiresDistrict: true,
      district: "4",
    });

    expect(mapWisconsinSunshineOffice({ office: "State Senate" })).toBeNull();
    expect(mapWisconsinSunshineOffice({ office: "State Assembly", district: "   " })).toBeNull();
  });

  it("maps app canonical offices to Wisconsin Sunshine search inputs", () => {
    expect(
      toWisconsinSunshineOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Attorney General",
      })
    ).toEqual({ sunshineOffice: "Attorney General", district: null });
    expect(
      toWisconsinSunshineOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "State Treasurer",
      })
    ).toEqual({ sunshineOffice: "State Treasurer", district: null });
    expect(
      toWisconsinSunshineOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        district: "9",
      })
    ).toEqual({ sunshineOffice: "State Assembly", district: "9" });
  });

  it("rejects unsafe Sunshine labels and incomplete app search inputs", () => {
    expect(mapWisconsinSunshineOffice({ office: "Supreme Court" })).toBeNull();
    expect(mapWisconsinSunshineOffice({ office: "Court of Appeals" })).toBeNull();
    expect(mapWisconsinSunshineOffice({ office: "Circuit Court" })).toBeNull();
    expect(mapWisconsinSunshineOffice({ office: "District Attorney" })).toBeNull();
    expect(mapWisconsinSunshineOffice({ office: "Local Office" })).toBeNull();
    expect(mapWisconsinSunshineOffice({ office: "" })).toBeNull();

    expect(
      toWisconsinSunshineOfficeSearchInput({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
      })
    ).toBeNull();
    expect(
      toWisconsinSunshineOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "State Supreme Court Justice",
      })
    ).toBeNull();
  });
});
