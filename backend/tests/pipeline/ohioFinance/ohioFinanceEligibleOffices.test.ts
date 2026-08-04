import { describe, expect, it } from "vitest";

import {
  OHIO_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isOhioFinanceEligibleOffice,
  toOhioFinanceOfficeKey,
} from "../../../src/pipeline/ohioFinance/ohioFinanceEligibleOffices.js";

describe("ohioFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Ohio finance office allowlist", () => {
    expect(OHIO_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Attorney General",
      "statewide::Secretary of State",
      "statewide::State Auditor",
      "statewide::State Treasurer",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("builds trimmed office keys", () => {
    expect(
      toOhioFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " State Auditor ",
      })
    ).toBe("statewide::State Auditor");
  });

  it("returns null for missing office key parts", () => {
    expect(toOhioFinanceOfficeKey({ officeScope: null, officeCanonicalName: "Governor" })).toBeNull();
    expect(toOhioFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: "" })).toBeNull();
    expect(toOhioFinanceOfficeKey({ officeScope: "  ", officeCanonicalName: "Governor" })).toBeNull();
  });

  it("allows every explicit eligible office", () => {
    for (const key of OHIO_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isOhioFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }
  });

  it("does not treat a broad scope as sufficient eligibility", () => {
    expect(isOhioFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Lieutenant Governor" })).toBe(false);
    expect(isOhioFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "State Level Judge" })).toBe(false);
    expect(isOhioFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "County Auditor" })).toBe(false);
    expect(isOhioFinanceEligibleOffice({ officeScope: "us_house", officeCanonicalName: "United States Representative" })).toBe(false);
    expect(isOhioFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Senator" })).toBe(false);
  });
});
