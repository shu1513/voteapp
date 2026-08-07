import { describe, expect, it } from "vitest";

import {
  GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isGeorgiaFinanceEligibleOffice,
  toGeorgiaFinanceOfficeKey,
} from "../../../src/pipeline/georgiaFinance/georgiaFinanceEligibleOffices.js";

describe("georgiaFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Georgia finance office allowlist", () => {
    expect(GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Secretary of State",
      "statewide::Attorney General",
      "statewide::Commissioner of Agriculture",
      "statewide::Commissioner of Insurance",
      "statewide::Labor Commissioner",
      "statewide::Superintendent of Public Instruction",
      "statewide::Public Service Commissioner",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("builds trimmed office keys", () => {
    expect(
      toGeorgiaFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
  });

  it("returns null for missing office key parts", () => {
    expect(toGeorgiaFinanceOfficeKey({ officeScope: null, officeCanonicalName: "Governor" })).toBeNull();
    expect(toGeorgiaFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: "" })).toBeNull();
    expect(toGeorgiaFinanceOfficeKey({ officeScope: "  ", officeCanonicalName: "Governor" })).toBeNull();
  });

  it("allows every explicit eligible office", () => {
    for (const key of GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isGeorgiaFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }
  });

  it("does not treat a broad scope as sufficient eligibility", () => {
    // United States Senator is GA 2026's only non-state statewide election
    // row — federal money belongs to the FEC, never this module.
    expect(isGeorgiaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "United States Senator" })).toBe(
      false
    );
    expect(
      isGeorgiaFinanceEligibleOffice({ officeScope: "us_house", officeCanonicalName: "United States Representative" })
    ).toBe(false);
    expect(isGeorgiaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "County Commissioner" })).toBe(false);
    expect(isGeorgiaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "District Attorney" })).toBe(false);
    expect(
      isGeorgiaFinanceEligibleOffice({ officeScope: "school_unified", officeCanonicalName: "School Board Member" })
    ).toBe(false);
    expect(isGeorgiaFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Senator" })).toBe(false);
  });
});
