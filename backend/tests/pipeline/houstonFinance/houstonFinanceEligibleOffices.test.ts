import { describe, expect, it } from "vitest";
import {
  HOUSTON_CITY_GEOID,
  isHoustonFinanceEligibleElection,
} from "../../../src/pipeline/houstonFinance/houstonFinanceEligibleOffices.js";

describe("Houston finance eligibility", () => {
  it("accepts only Houston Mayor elections", () => {
    expect(isHoustonFinanceEligibleElection({
      state: "TX", districtType: "place", geoidCompact: HOUSTON_CITY_GEOID,
      officeScope: "place", officeCanonicalName: "Mayor",
    })).toBe(true);
    expect(isHoustonFinanceEligibleElection({
      state: "TX", districtType: "place", geoidCompact: "4819000",
      officeScope: "place", officeCanonicalName: "Mayor",
    })).toBe(false);
    expect(isHoustonFinanceEligibleElection({
      state: "TX", districtType: "place", geoidCompact: HOUSTON_CITY_GEOID,
      officeScope: "place", officeCanonicalName: "City Council Member",
    })).toBe(false);
  });
});
