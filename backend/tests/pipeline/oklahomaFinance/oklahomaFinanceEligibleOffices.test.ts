import { describe, expect, it } from "vitest";

import {
  OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isOklahomaFinanceEligibleOffice,
  toOklahomaFinanceOfficeKey,
} from "../../../src/pipeline/oklahomaFinance/oklahomaFinanceEligibleOffices.js";

describe("oklahomaFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Oklahoma finance office allowlist", () => {
    expect(OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Secretary of State",
      "statewide::Attorney General",
      "statewide::State Treasurer",
      "statewide::State Auditor",
      "statewide::Superintendent of Public Instruction",
      "statewide::Commissioner of Insurance",
      "statewide::Labor Commissioner",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("builds trimmed office keys", () => {
    expect(
      toOklahomaFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
  });

  it("returns null for missing office key parts", () => {
    expect(toOklahomaFinanceOfficeKey({ officeScope: null, officeCanonicalName: "Governor" })).toBeNull();
    expect(toOklahomaFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: "" })).toBeNull();
    expect(toOklahomaFinanceOfficeKey({ officeScope: "  ", officeCanonicalName: "Governor" })).toBeNull();
  });

  it("allows every explicit eligible office", () => {
    for (const key of OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isOklahomaFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }
  });

  it("does not treat a broad scope as sufficient eligibility", () => {
    expect(
      isOklahomaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Corporation Commissioner",
      })
    ).toBe(false);
    expect(
      isOklahomaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Comptroller",
      })
    ).toBe(false);
    expect(
      isOklahomaFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
    expect(
      isOklahomaFinanceEligibleOffice({
        officeScope: "place",
        officeCanonicalName: "Mayor",
      })
    ).toBe(false);
    expect(
      isOklahomaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });
});
