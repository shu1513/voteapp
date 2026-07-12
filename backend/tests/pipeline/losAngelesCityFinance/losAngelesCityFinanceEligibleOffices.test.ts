import { describe, expect, it } from "vitest";
import {
  isLosAngelesCityFinanceEligibleElection,
  LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS,
  toLosAngelesEthicsOfficeName,
} from "../../../src/pipeline/losAngelesCityFinance/losAngelesCityFinanceEligibleOffices.js";

describe("Los Angeles City finance eligibility", () => {
  const mayor = {
    state: "CA",
    districtType: "place",
    geoidCompact: "0644000",
    officeScope: "place",
    officeCanonicalName: "Mayor",
  };
  it("accepts exact Los Angeles Phase 2 citywide office identities", () => {
    expect(LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "place::Mayor",
      "place::Municipal Attorney",
      "place::Municipal Controller",
    ]);
    expect(isLosAngelesCityFinanceEligibleElection(mayor)).toBe(true);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        officeCanonicalName: "Municipal Attorney",
      }),
    ).toBe(true);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        officeCanonicalName: "Municipal Controller",
      }),
    ).toBe(true);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        geoidCompact: "0666000",
      }),
    ).toBe(false);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        officeCanonicalName: "City Council Member",
      }),
    ).toBe(false);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        districtType: "county",
      }),
    ).toBe(false);
  });

  it("maps VoteApp canonical names to exact Ethics section names", () => {
    expect(
      toLosAngelesEthicsOfficeName({
        officeScope: "place",
        officeCanonicalName: "Mayor",
      }),
    ).toBe("Mayor");
    expect(
      toLosAngelesEthicsOfficeName({
        officeScope: "place",
        officeCanonicalName: "Municipal Attorney",
      }),
    ).toBe("City Attorney");
    expect(
      toLosAngelesEthicsOfficeName({
        officeScope: "place",
        officeCanonicalName: "Municipal Controller",
      }),
    ).toBe("City Controller");
    expect(
      toLosAngelesEthicsOfficeName({
        officeScope: "place",
        officeCanonicalName: "City Council Member",
      }),
    ).toBeNull();
  });
});
