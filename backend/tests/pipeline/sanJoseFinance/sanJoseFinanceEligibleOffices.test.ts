import { describe, expect, it } from "vitest";

import {
  isSanJoseCityFinanceEligibleElection,
  parseSanJoseCityCouncilSeatNumber,
  SAN_JOSE_CITY_GEOID,
} from "../../../src/pipeline/sanJoseFinance/sanJoseFinanceEligibleOffices.js";

const ELIGIBLE_COUNCIL = {
  state: "CA",
  districtType: "place",
  geoidCompact: SAN_JOSE_CITY_GEOID,
  officeScope: "place",
  officeCanonicalName: "City Council Member",
  officialBallotTitle: "Member, City Council, District 5",
};

describe("parseSanJoseCityCouncilSeatNumber", () => {
  it("parses the catalog's council title spellings", () => {
    // Actual local rows read "Member, City Council, District 5".
    expect(parseSanJoseCityCouncilSeatNumber("Member, City Council, District 5")).toBe(5);
    expect(parseSanJoseCityCouncilSeatNumber("City Council Member District 10")).toBe(10);
    expect(parseSanJoseCityCouncilSeatNumber("Councilmember, District 7")).toBe(7);
    expect(parseSanJoseCityCouncilSeatNumber("Member of the City Council, District No. 9")).toBe(9);
  });

  it("rejects out-of-range districts and non-council titles", () => {
    expect(parseSanJoseCityCouncilSeatNumber("Member, City Council, District 11")).toBeNull();
    expect(parseSanJoseCityCouncilSeatNumber("Member, City Council, District 0")).toBeNull();
    expect(parseSanJoseCityCouncilSeatNumber("Mayor")).toBeNull();
    expect(parseSanJoseCityCouncilSeatNumber("Member, Board of Supervisors, District 5")).toBeNull();
    expect(parseSanJoseCityCouncilSeatNumber("Member, City Council")).toBeNull();
    expect(parseSanJoseCityCouncilSeatNumber(null)).toBeNull();
    expect(parseSanJoseCityCouncilSeatNumber("")).toBeNull();
  });
});

describe("isSanJoseCityFinanceEligibleElection", () => {
  it("accepts San José council seats and the mayor", () => {
    expect(isSanJoseCityFinanceEligibleElection(ELIGIBLE_COUNCIL)).toBe(true);
    expect(
      isSanJoseCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officeCanonicalName: "Mayor",
        officialBallotTitle: "Mayor",
      }),
    ).toBe(true);
  });

  it("rejects rows outside the San José place scope", () => {
    expect(
      isSanJoseCityFinanceEligibleElection({ ...ELIGIBLE_COUNCIL, state: "TX" }),
    ).toBe(false);
    expect(
      isSanJoseCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        geoidCompact: "0667000", // San Francisco
      }),
    ).toBe(false);
    expect(
      isSanJoseCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        districtType: "county",
      }),
    ).toBe(false);
    expect(
      isSanJoseCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officeScope: "county",
      }),
    ).toBe(false);
    expect(
      isSanJoseCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officeCanonicalName: "County Supervisor",
      }),
    ).toBe(false);
  });

  it("requires a parseable council seat for council rows", () => {
    expect(
      isSanJoseCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officialBallotTitle: "Member, City Council, District 11",
      }),
    ).toBe(false);
    expect(
      isSanJoseCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officialBallotTitle: null,
      }),
    ).toBe(false);
  });
});
