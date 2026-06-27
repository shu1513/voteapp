import { describe, expect, it } from "vitest";

import {
  KENTUCKY_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isKentuckyFinanceEligibleOffice,
  normalizeKentuckyKrefLocation,
  toKentuckyFinanceOfficeKey,
} from "../../../src/pipeline/kentuckyFinance/kentuckyFinanceEligibleOffices.js";

describe("kentuckyFinanceEligibleOffices", () => {
  it("lists the Kentucky office keys currently eligible for KREF finance sync", () => {
    expect(KENTUCKY_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Secretary of State",
      "statewide::Attorney General",
      "statewide::State Treasurer",
      "statewide::State Auditor",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("identifies eligible office keys from app office metadata", () => {
    expect(toKentuckyFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(
      "statewide::Governor"
    );
    expect(isKentuckyFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(true);
    expect(
      isKentuckyFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(true);
    expect(isKentuckyFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBe(false);
    expect(
      isKentuckyFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Supreme Court Justice" })
    ).toBe(false);
    expect(isKentuckyFinanceEligibleOffice({ officeScope: " ", officeCanonicalName: "Governor" })).toBe(false);
  });

  it("normalizes KREF location text without guessing district semantics", () => {
    expect(normalizeKentuckyKrefLocation("  Statewide  ")).toBe("Statewide");
    expect(normalizeKentuckyKrefLocation("District   9")).toBe("District 9");
    expect(normalizeKentuckyKrefLocation("  ")).toBeNull();
    expect(normalizeKentuckyKrefLocation(null)).toBeNull();
  });
});
