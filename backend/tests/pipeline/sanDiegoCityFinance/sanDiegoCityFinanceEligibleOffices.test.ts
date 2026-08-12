import { describe, expect, it } from "vitest";

import {
  isSanDiegoCityFinanceEligibleElection,
  parseSanDiegoCityCouncilSeatNumber,
  SAN_DIEGO_CITY_GEOID,
} from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityFinanceEligibleOffices.js";

const ELIGIBLE_COUNCIL = {
  state: "CA",
  districtType: "place",
  geoidCompact: SAN_DIEGO_CITY_GEOID,
  officeScope: "place",
  officeCanonicalName: "City Council Member",
  officialBallotTitle: "Member of the City Council, District 2",
};

describe("parseSanDiegoCityCouncilSeatNumber", () => {
  it("parses the catalog's council title spellings", () => {
    // Actual local rows read "Member of the City Council, District 2".
    expect(parseSanDiegoCityCouncilSeatNumber("Member of the City Council, District 2")).toBe(2);
    expect(parseSanDiegoCityCouncilSeatNumber("City Council Member District 9")).toBe(9);
    expect(parseSanDiegoCityCouncilSeatNumber("Councilmember, District 7")).toBe(7);
    expect(parseSanDiegoCityCouncilSeatNumber("Member, City Council, District No. 4")).toBe(4);
  });

  it("rejects out-of-range districts and non-council titles", () => {
    // San Diego has nine council districts, not San José's ten.
    expect(parseSanDiegoCityCouncilSeatNumber("Member of the City Council, District 10")).toBeNull();
    expect(parseSanDiegoCityCouncilSeatNumber("Member of the City Council, District 0")).toBeNull();
    expect(parseSanDiegoCityCouncilSeatNumber("Mayor")).toBeNull();
    expect(parseSanDiegoCityCouncilSeatNumber("Member, Board of Supervisors, District 5")).toBeNull();
    expect(parseSanDiegoCityCouncilSeatNumber("Member of the City Council")).toBeNull();
    expect(parseSanDiegoCityCouncilSeatNumber(null)).toBeNull();
    expect(parseSanDiegoCityCouncilSeatNumber("")).toBeNull();
  });
});

describe("isSanDiegoCityFinanceEligibleElection", () => {
  it("accepts San Diego council seats and the mayor", () => {
    expect(isSanDiegoCityFinanceEligibleElection(ELIGIBLE_COUNCIL)).toBe(true);
    expect(
      isSanDiegoCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officeCanonicalName: "Mayor",
        officialBallotTitle: "Mayor",
      }),
    ).toBe(true);
  });

  it("rejects rows outside the San Diego place scope", () => {
    expect(
      isSanDiegoCityFinanceEligibleElection({ ...ELIGIBLE_COUNCIL, state: "TX" }),
    ).toBe(false);
    expect(
      isSanDiegoCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        geoidCompact: "0668000", // San José
      }),
    ).toBe(false);
    expect(
      isSanDiegoCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        districtType: "county",
      }),
    ).toBe(false);
    expect(
      isSanDiegoCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officeScope: "county",
      }),
    ).toBe(false);
    // Municipal Attorney is a real city office but deliberately outside the
    // Phase 2 whitelist (no live-validated resolver evidence model yet).
    expect(
      isSanDiegoCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officeCanonicalName: "Municipal Attorney",
        officialBallotTitle: "City Attorney",
      }),
    ).toBe(false);
    expect(
      isSanDiegoCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officeCanonicalName: "County Supervisor",
      }),
    ).toBe(false);
  });

  it("requires a parseable council seat for council rows", () => {
    expect(
      isSanDiegoCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officialBallotTitle: "Member of the City Council, District 10",
      }),
    ).toBe(false);
    expect(
      isSanDiegoCityFinanceEligibleElection({
        ...ELIGIBLE_COUNCIL,
        officialBallotTitle: null,
      }),
    ).toBe(false);
  });
});
