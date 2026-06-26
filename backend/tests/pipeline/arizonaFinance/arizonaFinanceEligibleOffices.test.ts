import { describe, expect, it } from "vitest";

import {
  isArizonaFinanceEligibleOffice,
  normalizeArizonaFinanceOffice,
} from "../../../src/pipeline/arizonaFinance/arizonaFinanceEligibleOffices.js";

describe("arizonaFinanceEligibleOffices", () => {
  it("allows the narrow Arizona state finance office set", () => {
    expect(isArizonaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(true);
    expect(isArizonaFinanceEligibleOffice({ officeScope: "state_upper", officeCanonicalName: "State Senator" })).toBe(true);
    expect(
      isArizonaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(true);
  });

  it("rejects unsupported Arizona offices", () => {
    expect(isArizonaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBe(false);
    expect(isArizonaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Corporation Commissioner" })).toBe(
      false
    );
  });

  it("normalizes common office labels", () => {
    expect(normalizeArizonaFinanceOffice({ officeScope: "statewide", officeName: "AG" })).toBeNull();
    expect(normalizeArizonaFinanceOffice({ officeScope: "statewide", officeName: "Attorney General" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
    });
    expect(normalizeArizonaFinanceOffice({ officeScope: "state_lower", officeName: "State House" })).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
    });
  });
});
