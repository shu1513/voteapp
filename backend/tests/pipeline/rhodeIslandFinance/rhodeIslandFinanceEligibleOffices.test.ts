import { describe, expect, it } from "vitest";

import {
  RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isRhodeIslandFinanceEligibleOffice,
  toRhodeIslandFinanceOfficeKey,
} from "../../../src/pipeline/rhodeIslandFinance/rhodeIslandFinanceEligibleOffices.js";

describe("rhodeIslandFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Rhode Island finance office allowlist", () => {
    expect(RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Secretary of State",
      "statewide::Attorney General",
      "statewide::State Treasurer",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("builds trimmed office keys", () => {
    expect(
      toRhodeIslandFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
  });

  it("returns null for missing office key parts", () => {
    expect(toRhodeIslandFinanceOfficeKey({ officeScope: null, officeCanonicalName: "Governor" })).toBeNull();
    expect(toRhodeIslandFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: "" })).toBeNull();
    expect(toRhodeIslandFinanceOfficeKey({ officeScope: "  ", officeCanonicalName: "Governor" })).toBeNull();
  });

  it("allows every explicit eligible office", () => {
    for (const key of RHODE_ISLAND_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isRhodeIslandFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }
  });

  it("uses the VoteApp canonical treasurer name, not Rhode Island's official title", () => {
    // RI's official title is "General Treasurer"; the VoteApp canonical
    // office (and the RI 2026 statewide election row) is "State Treasurer".
    // The raw title must never pass — matching on it would silently omit the
    // race everywhere the allowlist is consulted.
    expect(isRhodeIslandFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "General Treasurer" })).toBe(
      false
    );
    expect(isRhodeIslandFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "State Treasurer" })).toBe(true);
  });

  it("does not treat a broad scope as sufficient eligibility", () => {
    // United States Senator is on RI's 2026 statewide ballot — federal money
    // belongs to the FEC, never this module.
    expect(isRhodeIslandFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "United States Senator" })).toBe(
      false
    );
    expect(
      isRhodeIslandFinanceEligibleOffice({ officeScope: "us_house", officeCanonicalName: "United States Representative" })
    ).toBe(false);
    // Municipal offices are out of v1: paper filing is lawful for smaller
    // committees, so electronic coverage is unproven (rhode_island_plan.md
    // decision 9).
    expect(isRhodeIslandFinanceEligibleOffice({ officeScope: "place", officeCanonicalName: "Mayor" })).toBe(false);
    expect(isRhodeIslandFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Senator" })).toBe(false);
  });
});
