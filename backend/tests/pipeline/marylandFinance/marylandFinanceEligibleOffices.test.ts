import { describe, expect, it } from "vitest";

import {
  MARYLAND_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isMarylandFinanceEligibleOffice,
  toMarylandFinanceOfficeKey,
} from "../../../src/pipeline/marylandFinance/marylandFinanceEligibleOffices.js";

describe("marylandFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Maryland finance office allowlist", () => {
    expect(MARYLAND_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Attorney General",
      "statewide::Comptroller",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("builds trimmed office keys", () => {
    expect(
      toMarylandFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Comptroller ",
      })
    ).toBe("statewide::Comptroller");
  });

  it("returns null for missing office key parts", () => {
    expect(toMarylandFinanceOfficeKey({ officeScope: null, officeCanonicalName: "Governor" })).toBeNull();
    expect(toMarylandFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: "" })).toBeNull();
    expect(toMarylandFinanceOfficeKey({ officeScope: "  ", officeCanonicalName: "Governor" })).toBeNull();
  });

  it("allows every explicit eligible office", () => {
    for (const key of MARYLAND_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isMarylandFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }
  });

  it("does not treat a broad scope as sufficient eligibility", () => {
    expect(isMarylandFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "State Treasurer" })).toBe(false);
    expect(isMarylandFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBe(false);
    expect(isMarylandFinanceEligibleOffice({ officeScope: "place", officeCanonicalName: "Mayor" })).toBe(false);
    expect(isMarylandFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Senator" })).toBe(false);
  });
});
