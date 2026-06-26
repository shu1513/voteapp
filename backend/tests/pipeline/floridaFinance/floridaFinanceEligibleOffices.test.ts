import { describe, expect, it } from "vitest";

import {
  FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isFloridaFinanceEligibleOffice,
  toFloridaFinanceOfficeKey,
} from "../../../src/pipeline/floridaFinance/floridaFinanceEligibleOffices.js";

describe("floridaFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Florida finance office allowlist", () => {
    expect(FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Attorney General",
      "statewide::Chief Financial Officer",
      "statewide::Commissioner of Agriculture",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("allows statewide and legislative offices while rejecting federal and local guesses", () => {
    for (const key of FLORIDA_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isFloridaFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }

    expect(
      isFloridaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "United States Senator",
      })
    ).toBe(false);
    expect(
      isFloridaFinanceEligibleOffice({
        officeScope: "us_house",
        officeCanonicalName: "United States Representative",
      })
    ).toBe(false);
    expect(
      isFloridaFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
    expect(
      isFloridaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });

  it("builds trimmed office keys and rejects missing parts", () => {
    expect(
      toFloridaFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
    expect(toFloridaFinanceOfficeKey({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(toFloridaFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: " " })).toBeNull();
  });
});
