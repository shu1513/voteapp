import { describe, expect, it } from "vitest";

import {
  NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isNorthCarolinaFinanceEligibleOffice,
  toNorthCarolinaFinanceOfficeKey,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaFinanceEligibleOffices.js";

describe("northCarolinaFinanceEligibleOffices", () => {
  it("keeps a conservative explicit North Carolina finance office allowlist", () => {
    expect(NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("builds trimmed office keys", () => {
    expect(
      toNorthCarolinaFinanceOfficeKey({
        officeScope: " state_upper ",
        officeCanonicalName: " State Senator ",
      })
    ).toBe("state_upper::State Senator");
  });

  it("returns null for missing office key parts", () => {
    expect(toNorthCarolinaFinanceOfficeKey({ officeScope: null, officeCanonicalName: "State Senator" })).toBeNull();
    expect(toNorthCarolinaFinanceOfficeKey({ officeScope: "state_upper", officeCanonicalName: "" })).toBeNull();
    expect(toNorthCarolinaFinanceOfficeKey({ officeScope: "  ", officeCanonicalName: "State Senator" })).toBeNull();
  });

  it("allows every explicit eligible office", () => {
    for (const key of NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isNorthCarolinaFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }
  });

  it("does not treat a broad scope as sufficient eligibility", () => {
    // United States Senator is NC 2026's only statewide election row —
    // federal money belongs to the FEC, never this module.
    expect(isNorthCarolinaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "United States Senator" })).toBe(
      false
    );
    expect(isNorthCarolinaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(false);
    expect(isNorthCarolinaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "District Attorney" })).toBe(false);
    expect(isNorthCarolinaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "County Level Judge" })).toBe(false);
    expect(isNorthCarolinaFinanceEligibleOffice({ officeScope: "us_house", officeCanonicalName: "United States Representative" })).toBe(
      false
    );
    expect(isNorthCarolinaFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Senator" })).toBe(false);
  });
});
