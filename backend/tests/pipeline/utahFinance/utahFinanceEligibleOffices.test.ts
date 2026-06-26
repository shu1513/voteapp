import { describe, expect, it } from "vitest";

import {
  UTAH_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isUtahFinanceEligibleOffice,
  toUtahFinanceOfficeKey,
} from "../../../src/pipeline/utahFinance/utahFinanceEligibleOffices.js";

describe("utahFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Utah finance office allowlist", () => {
    expect(UTAH_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Attorney General",
      "statewide::State Auditor",
      "statewide::State Treasurer",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("allows explicit Utah offices and rejects broad guesses", () => {
    for (const key of UTAH_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isUtahFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }

    expect(isUtahFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "County Clerk" })).toBe(false);
    expect(isUtahFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Secretary of State" })).toBe(false);
    expect(isUtahFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Senator" })).toBe(false);
  });

  it("builds trimmed office keys and rejects missing pieces", () => {
    expect(toUtahFinanceOfficeKey({ officeScope: " statewide ", officeCanonicalName: " Governor " })).toBe(
      "statewide::Governor"
    );
    expect(toUtahFinanceOfficeKey({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(toUtahFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: " " })).toBeNull();
  });
});
