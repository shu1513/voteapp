import { describe, expect, it } from "vitest";

import {
  nevadaCandidateNamesMatch,
  nevadaDistrictNumberFromName,
  parseNevadaAuroraOffice,
  resolveNevadaCandidateFilers,
  type NevadaResolverCandidate,
  type NevadaRosterEntry,
} from "../../../src/pipeline/nevadaFinance/nevadaCandidateFilerResolver.js";
import type { NevadaReportListRow } from "../../../src/pipeline/nevadaFinance/nevadaReportSummary.js";

function reportRow(office: string, year = 2026): NevadaReportListRow {
  return { reportName: "CE Report 2", year, fileDate: "2026-07-15", office, syn: "token" };
}

function candidate(overrides: Partial<NevadaResolverCandidate>): NevadaResolverCandidate {
  return {
    candidateId: "candidate",
    electionId: "election",
    electionYear: 2026,
    candidateName: "Alexis Hansen",
    officeScope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    districtName: "Assembly District 32 (2024); Nevada",
    ...overrides,
  };
}

function roster(name: string, rows: NevadaReportListRow[]): NevadaRosterEntry {
  return { name, slug: name.toLowerCase().replace(/[^a-z0-9]+/g, "-"), detailToken: "o", reportRows: rows };
}

describe("nevadaCandidateNamesMatch", () => {
  it("matches the VoteApp-vs-AURORA name shapes seen in the fixtures", () => {
    expect(nevadaCandidateNamesMatch("Joe Lombardo", "Joseph Lombardo")).toBe(true);
    expect(nevadaCandidateNamesMatch("Alexis Hansen", "Alexis M Hansen")).toBe(true);
    expect(nevadaCandidateNamesMatch("Nicole Cannizzaro", "Nicole Jeanette Cannizzaro")).toBe(true);
    expect(nevadaCandidateNamesMatch("Douglas Herndon", "Douglas W Herndon")).toBe(true);
    expect(nevadaCandidateNamesMatch("Aaron Ford", "Aaron Darnell Ford")).toBe(true);
    expect(nevadaCandidateNamesMatch("Fabian Doñate", "Fabian Donate")).toBe(true);
    expect(nevadaCandidateNamesMatch("Cecelia González", "Cecelia Gonzalez")).toBe(true);
    expect(nevadaCandidateNamesMatch("Cisco Aguilar", "Francisco Aguilar")).toBe(true);
  });

  it("rejects different people", () => {
    expect(nevadaCandidateNamesMatch("Ira Hansen", "Alexis M Hansen")).toBe(false);
    expect(nevadaCandidateNamesMatch("Janine Hansen", "Alexis M Hansen")).toBe(false);
    expect(nevadaCandidateNamesMatch("Aaron Ford", "Danielle Ford")).toBe(false);
  });
});

describe("parseNevadaAuroraOffice", () => {
  it("maps AURORA office strings onto VoteApp office terms", () => {
    expect(parseNevadaAuroraOffice("State Senate, District 16")).toEqual({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      districtNumber: 16,
    });
    expect(parseNevadaAuroraOffice("State Assembly, District 32")).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      districtNumber: 32,
    });
    expect(parseNevadaAuroraOffice("State Assembly District 40")).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      districtNumber: 40,
    });
    expect(parseNevadaAuroraOffice("State Senate District 14")).toMatchObject({ districtNumber: 14 });
    // Filer-typed free-text variants seen live on county-jurisdiction filers.
    expect(parseNevadaAuroraOffice("Nevada State Assembly District 6")).toMatchObject({
      officeScope: "state_lower",
      districtNumber: 6,
    });
    expect(
      parseNevadaAuroraOffice("Assemblywoman, Nevada State Assembly District 41, Nevada State Legislature")
    ).toMatchObject({ officeScope: "state_lower", districtNumber: 41 });
    expect(
      parseNevadaAuroraOffice("Candidate for Nevada State Assembly, District 22 - State of Nevada")
    ).toMatchObject({ officeScope: "state_lower", districtNumber: 22 });
    expect(parseNevadaAuroraOffice("Treasurer, State of Nevada")).toMatchObject({
      officeCanonicalName: "State Treasurer",
    });
    expect(parseNevadaAuroraOffice("State Controller, Nevada")).toMatchObject({
      officeCanonicalName: "Comptroller",
    });
    // Chamber-ambiguous and out-of-jurisdiction text fails closed.
    expect(parseNevadaAuroraOffice("legislature district 27")).toBeNull();
    expect(parseNevadaAuroraOffice("Office Not Specified")).toBeNull();
    expect(parseNevadaAuroraOffice("Las Vegas City Council Ward 5")).toBeNull();
    expect(parseNevadaAuroraOffice("Nye County Treasurer")).toBeNull();
    expect(parseNevadaAuroraOffice("U.S. Senate")).toBeNull();
    // Chamber without a district parses with a null district (row is then
    // unusable for legislative confirmation).
    expect(parseNevadaAuroraOffice("Nevada State Assembly")).toMatchObject({
      officeScope: "state_lower",
      districtNumber: null,
    });
    expect(parseNevadaAuroraOffice("Governor")).toMatchObject({ officeCanonicalName: "Governor" });
    expect(parseNevadaAuroraOffice("State Controller")).toMatchObject({
      officeCanonicalName: "Comptroller",
    });
    expect(parseNevadaAuroraOffice("Supreme Court Justice, Seat D")).toMatchObject({
      officeCanonicalName: "State Level Judge",
    });
    expect(parseNevadaAuroraOffice("County Commissioner, District A")).toBeNull();
  });

  it("reads district numbers off VoteApp district names", () => {
    expect(nevadaDistrictNumberFromName("Assembly District 32 (2024); Nevada")).toBe(32);
    expect(nevadaDistrictNumberFromName("Nevada")).toBeNull();
  });
});

describe("resolveNevadaCandidateFilers", () => {
  it("links on unique name match confirmed by election-year report office, not the profile office", () => {
    // Cannizzaro pattern: current office is a senate seat, 2026 candidacy is
    // Attorney General; only the report rows prove the candidacy.
    const resolution = resolveNevadaCandidateFilers({
      candidates: [
        candidate({
          candidateName: "Nicole Cannizzaro",
          officeScope: "statewide",
          officeCanonicalName: "Attorney General",
          districtName: "Nevada",
        }),
      ],
      rosterEntries: [
        roster("Nicole Jeanette Cannizzaro", [
          reportRow("State Senate, District 6", 2024),
          reportRow("Attorney General", 2026),
        ]),
      ],
    });
    expect(resolution.matches).toHaveLength(1);
    expect(resolution.matches[0].confirmedOffice).toBe("Attorney General");
    expect(resolution.skips).toHaveLength(0);
  });

  it("skips on district mismatch, missing election-year rows, and ambiguity", () => {
    const wrongDistrict = resolveNevadaCandidateFilers({
      candidates: [candidate({})],
      rosterEntries: [roster("Alexis M Hansen", [reportRow("State Assembly, District 13")])],
    });
    expect(wrongDistrict.skips[0].reason).toBe("office_mismatch");

    const noRows = resolveNevadaCandidateFilers({
      candidates: [candidate({})],
      rosterEntries: [roster("Alexis M Hansen", [reportRow("State Assembly, District 32", 2024)])],
    });
    expect(noRows.skips[0].reason).toBe("no_election_year_reports");

    const unparseableDistrict = resolveNevadaCandidateFilers({
      candidates: [candidate({ districtName: "Nevada" })],
      rosterEntries: [roster("Alexis M Hansen", [reportRow("State Assembly, District 32")])],
    });
    expect(unparseableDistrict.matches).toHaveLength(0);
    expect(unparseableDistrict.skips[0].reason).toBe("office_mismatch");
    expect(unparseableDistrict.skips[0].detail).toMatch(/no parseable district/);

    const ambiguous = resolveNevadaCandidateFilers({
      candidates: [candidate({ candidateName: "Alexis Hansen" })],
      rosterEntries: [
        roster("Alexis M Hansen", [reportRow("State Assembly, District 32")]),
        roster("Alexis R Hansen", [reportRow("State Assembly, District 32")]),
      ],
    });
    expect(ambiguous.skips[0].reason).toBe("ambiguous_roster_match");

    const contested = resolveNevadaCandidateFilers({
      candidates: [
        candidate({ candidateId: "c1", candidateName: "Alexis Hansen" }),
        candidate({ candidateId: "c2", candidateName: "Alexis M Hansen" }),
      ],
      rosterEntries: [roster("Alexis M Hansen", [reportRow("State Assembly, District 32")])],
    });
    expect(contested.matches).toHaveLength(0);
    expect(contested.skips.every((skip) => skip.reason === "roster_entry_contested")).toBe(true);
  });
});
