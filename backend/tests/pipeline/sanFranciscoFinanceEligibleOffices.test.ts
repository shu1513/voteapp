import { describe, expect, it } from "vitest";
import {
  isSanFranciscoFinanceEligibleElection,
  parseSanFranciscoSupervisorDistrictNumber,
  toSanFranciscoContestCode,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoFinanceEligibleOffices.js";

const COUNTY = {
  state: "CA",
  districtType: "county",
  geoidCompact: "06075",
  officeScope: "county",
};
const CITY = {
  state: "CA",
  districtType: "place",
  geoidCompact: "0667000",
  officeScope: "place",
};
const SFUSD = {
  state: "CA",
  districtType: "school_unified",
  geoidCompact: "0634410",
  officeScope: "school_unified",
};

describe("parseSanFranciscoSupervisorDistrictNumber", () => {
  it("parses the local ballot-title shape", () => {
    expect(
      parseSanFranciscoSupervisorDistrictNumber(
        "Member, Board of Supervisors, District 10",
      ),
    ).toBe(10);
    expect(
      parseSanFranciscoSupervisorDistrictNumber(
        "Member, Board of Supervisors, District 4",
      ),
    ).toBe(4);
    expect(
      parseSanFranciscoSupervisorDistrictNumber("Supervisor, District 7"),
    ).toBe(7);
  });

  it("rejects out-of-range districts and other offices", () => {
    expect(
      parseSanFranciscoSupervisorDistrictNumber(
        "Member, Board of Supervisors, District 12",
      ),
    ).toBeNull();
    expect(parseSanFranciscoSupervisorDistrictNumber("Mayor")).toBeNull();
    expect(parseSanFranciscoSupervisorDistrictNumber(null)).toBeNull();
  });
});

describe("toSanFranciscoContestCode", () => {
  it("maps every covered office to its SFEC contest code", () => {
    const cases: [string, string, string][] = [
      ["place", "Mayor", "myr"],
      ["place", "Municipal Attorney", "cat"],
      ["place", "City Treasurer", "ttx"],
      ["county", "District Attorney", "dat"],
      ["county", "Sheriff", "shf"],
      ["county", "County Assessor-Recorder", "asr"],
      ["county", "Public Defender", "pdr"],
      ["school_unified", "School Board Member", "usd"],
    ];
    for (const [officeScope, officeCanonicalName, code] of cases) {
      expect(
        toSanFranciscoContestCode({ officeScope, officeCanonicalName }),
      ).toBe(code);
    }
  });

  it("zero-pads supervisor contest codes", () => {
    expect(
      toSanFranciscoContestCode({
        officeScope: "county",
        officeCanonicalName: "County Supervisor",
        supervisorDistrictNumber: 4,
      }),
    ).toBe("bos04");
    expect(
      toSanFranciscoContestCode({
        officeScope: "county",
        officeCanonicalName: "County Supervisor",
        supervisorDistrictNumber: 11,
      }),
    ).toBe("bos11");
    expect(
      toSanFranciscoContestCode({
        officeScope: "county",
        officeCanonicalName: "County Supervisor",
        supervisorDistrictNumber: null,
      }),
    ).toBeNull();
  });

  it("returns null for offices outside the covered set", () => {
    // Community College Board is deliberately deferred out of v1.
    expect(
      toSanFranciscoContestCode({
        officeScope: "county",
        officeCanonicalName: "County Treasurer",
      }),
    ).toBeNull();
    // Scope must match the confirmed placement, not just the name.
    expect(
      toSanFranciscoContestCode({
        officeScope: "county",
        officeCanonicalName: "Mayor",
      }),
    ).toBeNull();
    expect(
      toSanFranciscoContestCode({
        officeScope: "place",
        officeCanonicalName: "Sheriff",
      }),
    ).toBeNull();
  });
});

describe("isSanFranciscoFinanceEligibleElection", () => {
  it("accepts the eight Nov-2026 race shapes plus citywide offices", () => {
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...COUNTY,
        officeCanonicalName: "County Supervisor",
        officialBallotTitle: "Member, Board of Supervisors, District 4",
      }),
    ).toBe(true);
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...COUNTY,
        officeCanonicalName: "County Assessor-Recorder",
        officialBallotTitle: "Assessor-Recorder",
      }),
    ).toBe(true);
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...COUNTY,
        officeCanonicalName: "Public Defender",
        officialBallotTitle: "Public Defender",
      }),
    ).toBe(true);
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...CITY,
        officeCanonicalName: "Mayor",
        officialBallotTitle: "Mayor",
      }),
    ).toBe(true);
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...CITY,
        officeCanonicalName: "Municipal Attorney",
        officialBallotTitle: "City Attorney",
      }),
    ).toBe(true);
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...SFUSD,
        officeCanonicalName: "School Board Member",
        officialBallotTitle:
          "San Francisco Unified School District Board of Education Member",
      }),
    ).toBe(true);
  });

  it("rejects a supervisor race whose title has no parseable district", () => {
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...COUNTY,
        officeCanonicalName: "County Supervisor",
        officialBallotTitle: "Member, Board of Supervisors",
      }),
    ).toBe(false);
  });

  it("rejects other geographies and mismatched scopes", () => {
    // Los Angeles City mayor: same office key, wrong geoid.
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...CITY,
        geoidCompact: "0644000",
        officeCanonicalName: "Mayor",
        officialBallotTitle: "Mayor",
      }),
    ).toBe(false);
    // Another CA county's sheriff.
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...COUNTY,
        geoidCompact: "06001",
        officeCanonicalName: "Sheriff",
        officialBallotTitle: "Sheriff",
      }),
    ).toBe(false);
    // Not California.
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...COUNTY,
        state: "NV",
        officeCanonicalName: "Sheriff",
        officialBallotTitle: "Sheriff",
      }),
    ).toBe(false);
    // Office scope must equal the district type it sits in.
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...COUNTY,
        officeScope: "place",
        officeCanonicalName: "Mayor",
        officialBallotTitle: "Mayor",
      }),
    ).toBe(false);
    // SF ballot measures live under place with no office.
    expect(
      isSanFranciscoFinanceEligibleElection({
        ...CITY,
        officeCanonicalName: null,
        officialBallotTitle: "TBD: Affordable Housing Guarantee Act",
      }),
    ).toBe(false);
  });
});
