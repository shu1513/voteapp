import { describe, expect, it } from "vitest";
import {
  isPhoenixCityFinanceEligibleElection,
  parsePhoenixCityCouncilDistrictNumber,
} from "../../../src/pipeline/phoenixFinance/phoenixFinanceEligibleOffices.js";

describe("parsePhoenixCityCouncilDistrictNumber", () => {
  it("parses the local catalog title", () => {
    expect(
      parsePhoenixCityCouncilDistrictNumber("Phoenix City Council, District 4"),
    ).toBe(4);
    expect(
      parsePhoenixCityCouncilDistrictNumber("Council Member, District 8"),
    ).toBe(8);
  });

  it("rejects out-of-range districts and unrelated titles", () => {
    // Phoenix has eight council districts.
    expect(
      parsePhoenixCityCouncilDistrictNumber("Phoenix City Council, District 9"),
    ).toBeNull();
    expect(parsePhoenixCityCouncilDistrictNumber("Mayor")).toBeNull();
    expect(parsePhoenixCityCouncilDistrictNumber(null)).toBeNull();
  });
});

describe("isPhoenixCityFinanceEligibleElection", () => {
  const council = {
    state: "AZ",
    districtType: "place",
    geoidCompact: "0455000",
    officeScope: "place",
    officeCanonicalName: "City Council Member",
    officialBallotTitle: "Phoenix City Council, District 6",
  };

  it("accepts Phoenix council seats with a parseable district and Mayor", () => {
    expect(isPhoenixCityFinanceEligibleElection(council)).toBe(true);
    expect(
      isPhoenixCityFinanceEligibleElection({
        ...council,
        officeCanonicalName: "Mayor",
        officialBallotTitle: null,
      }),
    ).toBe(true);
  });

  it("rejects other geographies, scopes, offices, and unparseable districts", () => {
    expect(
      isPhoenixCityFinanceEligibleElection({ ...council, state: "CA" }),
    ).toBe(false);
    expect(
      isPhoenixCityFinanceEligibleElection({
        ...council,
        // Another AZ city's place row must never sweep in (the shared
        // due-list scoping lesson).
        geoidCompact: "0477000",
      }),
    ).toBe(false);
    expect(
      isPhoenixCityFinanceEligibleElection({ ...council, officeScope: "county" }),
    ).toBe(false);
    expect(
      isPhoenixCityFinanceEligibleElection({
        ...council,
        officeCanonicalName: "City Clerk",
      }),
    ).toBe(false);
    expect(
      isPhoenixCityFinanceEligibleElection({
        ...council,
        officialBallotTitle: "Phoenix City Council",
      }),
    ).toBe(false);
  });
});
