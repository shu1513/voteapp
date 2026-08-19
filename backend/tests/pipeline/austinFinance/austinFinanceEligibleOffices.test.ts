import { describe, expect, it } from "vitest";
import {
  AUSTIN_FINANCE_ELECTION_DATES,
  austinOfficeCodeDistrictLabel,
  austinOfficeCodeForElection,
  isAustinFinanceEligibleElection,
  isAustinFinanceSupportedElectionDate,
  parseAustinOfficeSoughtCode,
} from "../../../src/pipeline/austinFinance/austinFinanceEligibleOffices.js";

describe("austinOfficeCodeForElection", () => {
  it("reads the district number from roster council titles", () => {
    expect(
      austinOfficeCodeForElection({
        officeCanonicalName: "City Council Member",
        officialBallotTitle: "City Council Member District 1",
      }),
    ).toBe("COUNCIL_MBR_DISTRICT_01");
    expect(
      austinOfficeCodeForElection({
        officeCanonicalName: "City Council Member",
        officialBallotTitle: "City Council Member, District 10",
      }),
    ).toBe("COUNCIL_MBR_DISTRICT_10");
    expect(
      austinOfficeCodeForElection({
        officeCanonicalName: "Mayor",
        officialBallotTitle: "Mayor",
      }),
    ).toBe("MAYOR");
    // Mayor needs no title.
    expect(
      austinOfficeCodeForElection({ officeCanonicalName: "Mayor", officialBallotTitle: null }),
    ).toBe("MAYOR");
  });

  it("fails closed on missing, out-of-range, or conflicting district numbers and other offices", () => {
    expect(
      austinOfficeCodeForElection({
        officeCanonicalName: "City Council Member",
        officialBallotTitle: "City Council Member",
      }),
    ).toBeNull();
    expect(
      austinOfficeCodeForElection({
        officeCanonicalName: "City Council Member",
        officialBallotTitle: "City Council Member District 11",
      }),
    ).toBeNull();
    expect(
      austinOfficeCodeForElection({
        officeCanonicalName: "City Council Member",
        officialBallotTitle: "City Council Member District 0",
      }),
    ).toBeNull();
    expect(
      austinOfficeCodeForElection({
        officeCanonicalName: "City Council Member",
        officialBallotTitle: "City Council Member District 1 and District 2",
      }),
    ).toBeNull();
    expect(
      austinOfficeCodeForElection({
        officeCanonicalName: "City Controller",
        officialBallotTitle: "City Controller District 1",
      }),
    ).toBeNull();
    expect(
      austinOfficeCodeForElection({ officeCanonicalName: null, officialBallotTitle: "Mayor" }),
    ).toBeNull();
  });
});

describe("parseAustinOfficeSoughtCode", () => {
  it("reads the leading code through every live drift spelling", () => {
    for (const value of [
      "COUNCIL_MBR_DISTRICT_01",
      "COUNCIL_MBR_DISTRICT_01 District 1",
      "COUNCIL_MBR_DISTRICT_01 District One",
      "COUNCIL_MBR_DISTRICT_01 District District 1",
      " council_mbr_district_01 ",
    ])
      expect(parseAustinOfficeSoughtCode(value)).toBe("COUNCIL_MBR_DISTRICT_01");
    expect(parseAustinOfficeSoughtCode("COUNCIL_MBR_DISTRICT_10 District 10")).toBe(
      "COUNCIL_MBR_DISTRICT_10",
    );
    expect(parseAustinOfficeSoughtCode("MAYOR")).toBe("MAYOR");
    expect(parseAustinOfficeSoughtCode("MAYOR District Austin")).toBe("MAYOR");
  });

  it("returns null for NONE, OTHER, blanks, embedded codes, and out-of-range districts", () => {
    expect(parseAustinOfficeSoughtCode("NONE")).toBeNull();
    expect(parseAustinOfficeSoughtCode("NONE District 8")).toBeNull();
    expect(parseAustinOfficeSoughtCode("OTHER")).toBeNull();
    expect(parseAustinOfficeSoughtCode(null)).toBeNull();
    expect(parseAustinOfficeSoughtCode("")).toBeNull();
    // Only the LEADING code counts.
    expect(parseAustinOfficeSoughtCode("District 4 COUNCIL_MBR_DISTRICT_04")).toBeNull();
    expect(parseAustinOfficeSoughtCode("COUNCIL_MBR_DISTRICT_11")).toBeNull();
    expect(parseAustinOfficeSoughtCode("COUNCIL_MBR_DISTRICT_00")).toBeNull();
    expect(parseAustinOfficeSoughtCode("COUNCIL_MBR_DISTRICT_1")).toBeNull();
    expect(parseAustinOfficeSoughtCode("MAYORAL")).toBeNull();
  });

  it("meets the roster parser on one code", () => {
    expect(parseAustinOfficeSoughtCode("COUNCIL_MBR_DISTRICT_09 District 9")).toBe(
      austinOfficeCodeForElection({
        officeCanonicalName: "City Council Member",
        officialBallotTitle: "City Council Member District 9",
      }),
    );
  });
});

describe("austinOfficeCodeDistrictLabel", () => {
  it("renders the standard district column", () => {
    expect(austinOfficeCodeDistrictLabel("COUNCIL_MBR_DISTRICT_01")).toBe("District 1");
    expect(austinOfficeCodeDistrictLabel("COUNCIL_MBR_DISTRICT_10")).toBe("District 10");
    expect(austinOfficeCodeDistrictLabel("MAYOR")).toBeNull();
  });
});

describe("isAustinFinanceSupportedElectionDate", () => {
  it("accepts only the v1 allowlist", () => {
    expect(AUSTIN_FINANCE_ELECTION_DATES).toEqual(["2026-11-03"]);
    expect(isAustinFinanceSupportedElectionDate("2026-11-03")).toBe(true);
    expect(isAustinFinanceSupportedElectionDate("2024-11-05")).toBe(false);
    expect(isAustinFinanceSupportedElectionDate(null)).toBe(false);
  });
});

describe("isAustinFinanceEligibleElection", () => {
  const eligible = {
    state: "TX",
    districtType: "place",
    geoidCompact: "4805000",
    officeScope: "place",
    officeCanonicalName: "City Council Member",
    officialBallotTitle: "City Council Member District 5",
  };

  it("accepts Austin council district seats and Mayor on the place row", () => {
    expect(isAustinFinanceEligibleElection(eligible)).toBe(true);
    expect(isAustinFinanceEligibleElection({ ...eligible, state: " tx " })).toBe(true);
    expect(
      isAustinFinanceEligibleElection({
        ...eligible,
        officeCanonicalName: "Mayor",
        officialBallotTitle: "Mayor",
      }),
    ).toBe(true);
  });

  it("rejects wrong state, district, scope, office, and district-less council titles", () => {
    expect(isAustinFinanceEligibleElection({ ...eligible, state: "CO" })).toBe(false);
    expect(isAustinFinanceEligibleElection({ ...eligible, districtType: "county" })).toBe(false);
    // Houston is the other Texas city module — a different place row.
    expect(isAustinFinanceEligibleElection({ ...eligible, geoidCompact: "4835000" })).toBe(false);
    expect(isAustinFinanceEligibleElection({ ...eligible, officeScope: "county" })).toBe(false);
    expect(
      isAustinFinanceEligibleElection({ ...eligible, officeCanonicalName: "City Controller" }),
    ).toBe(false);
    expect(
      isAustinFinanceEligibleElection({ ...eligible, officialBallotTitle: "City Council Member" }),
    ).toBe(false);
    expect(isAustinFinanceEligibleElection({ ...eligible, officialBallotTitle: null })).toBe(false);
  });
});
