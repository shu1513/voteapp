import { describe, expect, it } from "vitest";
import {
  DENVER_CITY_GEOID,
  isDenverFinanceEligibleElection,
  parseDenverAtLargeSeatLetter,
} from "../../../src/pipeline/denverFinance/denverFinanceEligibleOffices.js";

describe("parseDenverAtLargeSeatLetter", () => {
  it("reads the seat letter from roster titles and SearchLight officeSought", () => {
    // SearchLight cycle-36 officeSought, verified live 2026-08-12.
    expect(parseDenverAtLargeSeatLetter("City Council At-Large Seat B")).toBe("B");
    // Catalog-style ballot titles.
    expect(
      parseDenverAtLargeSeatLetter("City Council Member, At-Large Seat A"),
    ).toBe("A");
    expect(
      parseDenverAtLargeSeatLetter("Member of the City Council, At Large Seat B"),
    ).toBe("B");
  });

  it("returns null for district seats, Mayor, and blanks", () => {
    expect(parseDenverAtLargeSeatLetter("City Council District 7")).toBeNull();
    expect(parseDenverAtLargeSeatLetter("Mayor")).toBeNull();
    // At-large without a seat letter is not enough — the gate needs the seat.
    expect(parseDenverAtLargeSeatLetter("City Council At-Large")).toBeNull();
    expect(parseDenverAtLargeSeatLetter("")).toBeNull();
    expect(parseDenverAtLargeSeatLetter(null)).toBeNull();
  });
});

describe("isDenverFinanceEligibleElection", () => {
  const eligible = {
    state: "CO",
    districtType: "place",
    geoidCompact: DENVER_CITY_GEOID,
    officeScope: "place",
    officeCanonicalName: "City Council Member",
    officialBallotTitle: "City Council Member, At-Large Seat B",
  };

  it("accepts the cycle-36 at-large council contest", () => {
    expect(isDenverFinanceEligibleElection(eligible)).toBe(true);
  });

  it("rejects wrong state, district, scope, office, and seatless titles", () => {
    expect(
      isDenverFinanceEligibleElection({ ...eligible, state: "CA" }),
    ).toBe(false);
    expect(
      isDenverFinanceEligibleElection({ ...eligible, districtType: "county" }),
    ).toBe(false);
    expect(
      isDenverFinanceEligibleElection({ ...eligible, geoidCompact: "0666000" }),
    ).toBe(false);
    expect(
      isDenverFinanceEligibleElection({ ...eligible, officeScope: "county" }),
    ).toBe(false);
    expect(
      isDenverFinanceEligibleElection({ ...eligible, officeCanonicalName: "Mayor" }),
    ).toBe(false);
    // District council seats stay out until the 2027-cycle work widens the gate.
    expect(
      isDenverFinanceEligibleElection({
        ...eligible,
        officialBallotTitle: "City Council Member, District 7",
      }),
    ).toBe(false);
    expect(
      isDenverFinanceEligibleElection({ ...eligible, officialBallotTitle: null }),
    ).toBe(false);
  });
});
