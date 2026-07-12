import { describe, expect, it } from "vitest";
import {
  HOUSTON_CITY_GEOID,
  isHoustonFinanceEligibleElection,
} from "../../../src/pipeline/houstonFinance/houstonFinanceEligibleOffices.js";

describe("Houston finance eligibility", () => {
  it("accepts supported Houston city offices only", () => {
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
    })).toBe(true);
    expect(isHoustonFinanceEligibleElection({
      state: "TX", districtType: "place", geoidCompact: HOUSTON_CITY_GEOID,
      officeScope: "place", officeCanonicalName: "Municipal Controller",
    })).toBe(true);
    expect(isHoustonFinanceEligibleElection({
      state: "TX", districtType: "place", geoidCompact: HOUSTON_CITY_GEOID,
      officeScope: "place", officeCanonicalName: "City Treasurer",
    })).toBe(false);
  });
});
