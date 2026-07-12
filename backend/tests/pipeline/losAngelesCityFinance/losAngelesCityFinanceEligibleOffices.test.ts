import { describe, expect, it } from "vitest";
import {
  isLosAngelesCityFinanceEligibleElection,
  LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES,
  LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS,
  LOS_ANGELES_UNIFIED_SCHOOL_DISTRICT_GEOID,
  parseLosAngelesCityCouncilSeatNumber,
  parseLosAngelesSchoolBoardSeatNumber,
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
    expect(LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_NAMES).toEqual([
      "Mayor",
      "Municipal Attorney",
      "Municipal Controller",
      "City Council Member",
      "School Board Member",
    ]);
    expect(LOS_ANGELES_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "place::Mayor",
      "place::Municipal Attorney",
      "place::Municipal Controller",
      "place::City Council Member",
      "school_unified::School Board Member",
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
        officialBallotTitle: "Member of the City Council, District No. 3",
      }),
    ).toBe(true);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        districtType: "county",
      }),
    ).toBe(false);
  });

  it("accepts only exact LAUSD school-board identity and seat", () => {
    const lausd = {
      state: "CA",
      districtType: "school_unified",
      geoidCompact: LOS_ANGELES_UNIFIED_SCHOOL_DISTRICT_GEOID,
      officeScope: "school_unified",
      officeCanonicalName: "School Board Member",
      officialBallotTitle: "Member of the Board of Education, District 6",
    };
    expect(isLosAngelesCityFinanceEligibleElection(lausd)).toBe(true);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...lausd,
        geoidCompact: "0600001",
      }),
    ).toBe(false);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...lausd,
        districtType: "place",
        geoidCompact: "0644000",
      }),
    ).toBe(false);
    expect(
      toLosAngelesEthicsOfficeName({
        officeScope: "school_unified",
        officeCanonicalName: "School Board Member",
        seatNumber: 6,
      }),
    ).toBe("LAUSD District 6");
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
        seatNumber: 11,
      }),
    ).toBe("Council District 11");
    expect(
      toLosAngelesEthicsOfficeName({
        officeScope: "place",
        officeCanonicalName: "City Council Member",
      }),
    ).toBeNull();
  });

  it("parses only recognized council titles and all seats 1 through 15", () => {
    for (let seat = 1; seat <= 15; seat += 1) {
      expect(
        parseLosAngelesCityCouncilSeatNumber(
          `Member of the City Council, District No. ${seat}`,
        ),
      ).toBe(seat);
    }
    expect(
      parseLosAngelesCityCouncilSeatNumber("City Council District 3"),
    ).toBe(3);
    expect(
      parseLosAngelesCityCouncilSeatNumber("Councilmember, District 11"),
    ).toBe(11);
    expect(
      parseLosAngelesCityCouncilSeatNumber("Council District 1 and 11"),
    ).toBeNull();
    expect(
      parseLosAngelesCityCouncilSeatNumber("Council District 0"),
    ).toBeNull();
    expect(
      parseLosAngelesCityCouncilSeatNumber("Council District 16"),
    ).toBeNull();
    expect(parseLosAngelesCityCouncilSeatNumber("District 3")).toBeNull();
  });

  it("rejects council elections with missing or malformed seat titles", () => {
    for (const officialBallotTitle of [
      null,
      "City Council Member",
      "Council District 1 and District 11",
      "Council District 16",
    ]) {
      expect(
        isLosAngelesCityFinanceEligibleElection({
          ...mayor,
          officeCanonicalName: "City Council Member",
          officialBallotTitle,
        }),
      ).toBe(false);
    }
  });

  it("parses only recognized LAUSD board titles and seats 1 through 7", () => {
    for (let seat = 1; seat <= 7; seat += 1)
      expect(
        parseLosAngelesSchoolBoardSeatNumber(
          `Member of the Board of Education, District ${seat}`,
        ),
      ).toBe(seat);
    expect(
      parseLosAngelesSchoolBoardSeatNumber("School Board District 4"),
    ).toBe(4);
    expect(parseLosAngelesSchoolBoardSeatNumber("District 6")).toBeNull();
    expect(
      parseLosAngelesSchoolBoardSeatNumber("Board of Education District 8"),
    ).toBeNull();
  });
});
