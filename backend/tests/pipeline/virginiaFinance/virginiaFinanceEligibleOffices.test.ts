import { describe, expect, it } from "vitest";

import {
  VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isVirginiaFinanceEligibleOffice,
  toVirginiaFinanceOfficeKey,
} from "../../../src/pipeline/virginiaFinance/virginiaFinanceEligibleOffices.js";

describe("virginiaFinanceEligibleOffices", () => {
  it("keeps a narrow explicit Virginia finance office allowlist", () => {
    expect(VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Attorney General",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("builds trimmed office keys", () => {
    expect(
      toVirginiaFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
  });

  it("returns null for missing office key parts", () => {
    expect(toVirginiaFinanceOfficeKey({ officeScope: null, officeCanonicalName: "Governor" })).toBeNull();
    expect(toVirginiaFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: "" })).toBeNull();
    expect(toVirginiaFinanceOfficeKey({ officeScope: "  ", officeCanonicalName: "Governor" })).toBeNull();
  });

  it("allows every explicit eligible office", () => {
    for (const key of VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isVirginiaFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }
  });

  it("does not treat broad Virginia scopes as sufficient eligibility", () => {
    expect(
      isVirginiaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Secretary of State",
      })
    ).toBe(false);
    expect(
      isVirginiaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Treasurer",
      })
    ).toBe(false);
    expect(
      isVirginiaFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
    expect(
      isVirginiaFinanceEligibleOffice({
        officeScope: "place",
        officeCanonicalName: "Mayor",
      })
    ).toBe(false);
    expect(
      isVirginiaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });
});
