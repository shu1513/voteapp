import { describe, expect, it } from "vitest";

import {
  TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isTexasFinanceEligibleOffice,
  mapTexasTecOfficeCode,
  toTexasFinanceOfficeKey,
} from "../../../src/pipeline/texasFinance/texasFinanceEligibleOffices.js";

describe("texasFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Texas finance office allowlist", () => {
    expect(TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Attorney General",
      "statewide::Comptroller",
      "statewide::Agriculture Commissioner",
      "statewide::Land Commissioner",
      "statewide::Railroad Commissioner",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("allows every explicit eligible office and rejects broad-scope guesses", () => {
    for (const key of TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isTexasFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }

    expect(
      isTexasFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Board of Education",
      })
    ).toBe(false);
    expect(
      isTexasFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
    expect(
      isTexasFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });

  it("builds trimmed office keys and rejects missing parts", () => {
    expect(
      toTexasFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
    expect(toTexasFinanceOfficeKey({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(toTexasFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: " " })).toBeNull();
  });

  it("maps safe TEC office codes into app canonical offices", () => {
    expect(mapTexasTecOfficeCode({ officeCode: "GOVERNOR" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      requiresDistrict: false,
    });
    expect(mapTexasTecOfficeCode({ officeCode: "RRCOMM_UNEXPIRED" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Railroad Commissioner",
    });
    expect(mapTexasTecOfficeCode({ officeCode: "STATESEN" })).toMatchObject({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      requiresDistrict: true,
    });
    expect(mapTexasTecOfficeCode({ officeCode: "STATEEDU" })).toBeNull();
  });
});
