import { describe, expect, it } from "vitest";

import {
  isNewHampshireFinanceEligibleOffice,
  toNewHampshireFinanceOfficeKey,
} from "../../../src/pipeline/newHampshireFinance/newHampshireFinanceEligibleOffices.js";

describe("newHampshireFinanceEligibleOffices", () => {
  it.each([
    ["statewide", "Governor"],
    ["state_upper", "State Senator"],
    ["state_lower", "State Lower Chamber Legislator"],
    ["county", "County Commissioner"],
    ["county", "District Attorney"],
    ["county", "County Treasurer"],
    ["county", "Sheriff"],
    ["county", "County Recorder"],
    ["county", "Clerk of Court"],
  ])("allows exact resolver-backed office %s::%s", (officeScope, officeCanonicalName) => {
    expect(isNewHampshireFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
  });

  it.each([
    ["statewide", "United States Senator"],
    ["statewide", "Executive Council"],
    ["county", "Recorder of Deeds"],
    ["place", "Mayor"],
    [null, "Governor"],
    ["statewide", null],
  ])("rejects unsupported or incomplete office %s::%s", (officeScope, officeCanonicalName) => {
    expect(isNewHampshireFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(false);
  });

  it("normalizes surrounding whitespace only", () => {
    expect(
      toNewHampshireFinanceOfficeKey({
        officeScope: " state_upper ",
        officeCanonicalName: " State Senator ",
      })
    ).toBe("state_upper::State Senator");
    expect(
      isNewHampshireFinanceEligibleOffice({
        officeScope: "STATE_UPPER",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });
});
