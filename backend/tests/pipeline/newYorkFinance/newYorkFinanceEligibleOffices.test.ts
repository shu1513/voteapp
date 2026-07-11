import { describe, expect, it } from "vitest";

import {
  NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isNewYorkFinanceEligibleOffice,
  normalizeNewYorkDistrict,
  toNewYorkBoeOfficeSearchInput,
} from "../../../src/pipeline/newYorkFinance/newYorkFinanceEligibleOffices.js";

describe("newYorkFinanceEligibleOffices", () => {
  it("gates to the six state offices only", () => {
    expect(NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEYS).toHaveLength(6);
    expect(isNewYorkFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(true);
    expect(isNewYorkFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Comptroller" })).toBe(true);
    expect(isNewYorkFinanceEligibleOffice({ officeScope: "state_upper", officeCanonicalName: "State Senator" })).toBe(
      true
    );
    expect(
      isNewYorkFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })
    ).toBe(true);
    // NYC offices belong to the NYC CFB; county offices are out of scope.
    expect(isNewYorkFinanceEligibleOffice({ officeScope: "city", officeCanonicalName: "Mayor" })).toBe(false);
    expect(isNewYorkFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Comptroller" })).toBe(false);
    expect(isNewYorkFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Secretary of State" })).toBe(
      false
    );
    expect(isNewYorkFinanceEligibleOffice({ officeScope: null, officeCanonicalName: "Governor" })).toBe(false);
  });

  it("normalizes NYSBOE district strings to bare numbers", () => {
    expect(normalizeNewYorkDistrict("43")).toBe("43");
    expect(normalizeNewYorkDistrict("043")).toBe("43");
    expect(normalizeNewYorkDistrict("AD 70")).toBe("70");
    expect(normalizeNewYorkDistrict("District 7")).toBe("7");
    expect(normalizeNewYorkDistrict("150")).toBe("150");
    expect(normalizeNewYorkDistrict("151")).toBeNull();
    expect(normalizeNewYorkDistrict("0")).toBeNull();
    expect(normalizeNewYorkDistrict("")).toBeNull();
    expect(normalizeNewYorkDistrict(null)).toBeNull();
    expect(normalizeNewYorkDistrict("statewide")).toBeNull();
  });

  it("maps app offices to NYSBOE labels, requiring districts for the legislature", () => {
    expect(
      toNewYorkBoeOfficeSearchInput({ officeScope: "statewide", officeCanonicalName: "Governor" })
    ).toEqual({ boeOfficeLabels: ["Governor"], district: null });

    // App-canonical Comptroller maps to the registry's statewide label only;
    // bare "Comptroller" in the registry is the county office.
    expect(
      toNewYorkBoeOfficeSearchInput({ officeScope: "statewide", officeCanonicalName: "Comptroller" })
    ).toEqual({ boeOfficeLabels: ["State Comptroller"], district: null });

    expect(
      toNewYorkBoeOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        district: "070",
      })
    ).toEqual({ boeOfficeLabels: ["Member of Assembly"], district: "70" });

    expect(
      toNewYorkBoeOfficeSearchInput({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
        district: null,
      })
    ).toBeNull();

    expect(
      toNewYorkBoeOfficeSearchInput({ officeScope: "statewide", officeCanonicalName: "Secretary of State" })
    ).toBeNull();
  });
});
