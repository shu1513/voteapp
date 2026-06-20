import { describe, expect, it } from "vitest";

import { isCaliforniaFinanceEligibleOffice } from "../../../src/pipeline/californiaFinance/californiaFinanceEligibleOffices.js";

describe("californiaFinanceEligibleOffices", () => {
  it("allows only the explicit CAL-ACCESS-safe California office set", () => {
    expect(
      isCaliforniaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Governor",
      })
    ).toBe(true);
    expect(
      isCaliforniaFinanceEligibleOffice({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
      })
    ).toBe(true);
    expect(
      isCaliforniaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(true);
    expect(
      isCaliforniaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Board of Equalization Member",
      })
    ).toBe(true);

    expect(
      isCaliforniaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "United States Senator",
      })
    ).toBe(false);
    expect(
      isCaliforniaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Auditor",
      })
    ).toBe(false);
    expect(
      isCaliforniaFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
  });
});
