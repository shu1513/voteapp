import { describe, expect, it } from "vitest";

import type { MontanaCersCandidateSearchRow } from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import {
  montanaCersCandidateDisplayName,
  montanaCersOfficeTitleMatches,
  resolveMontanaCersCandidate,
  toMontanaCersOfficeExpectation,
} from "../../../src/pipeline/montanaFinance/montanaCandidateCersResolver.js";

function row(overrides: Partial<MontanaCersCandidateSearchRow>): MontanaCersCandidateSearchRow {
  return {
    candidateId: 21020,
    lastName: "Bedey",
    firstName: "David",
    middleInitial: "F.",
    electionYear: 2026,
    officeTitle: "Senate District No. 43",
    officeCode: "236",
    partyDescr: "Republican",
    candidateStatusDescr: "Amended",
    resCountyDescr: "Ravalli",
    ...overrides,
  };
}

const SD43 = {
  candidateName: "David Bedey",
  electionYear: 2026,
  officeScope: "state_upper",
  officeName: "State Senator",
  districtName: "State Senate District 43 (2024); Montana",
  legislativeDistrict: "43",
};

describe("toMontanaCersOfficeExpectation", () => {
  it("maps the four eligible office classes and demands district numbers", () => {
    expect(
      toMontanaCersOfficeExpectation({ ...SD43 })
    ).toEqual({ kind: "legislative_upper", districtNumber: 43 });
    expect(
      toMontanaCersOfficeExpectation({
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        districtName: null,
        legislativeDistrict: "7",
      })
    ).toEqual({ kind: "legislative_lower", districtNumber: 7 });
    expect(
      toMontanaCersOfficeExpectation({
        officeScope: "statewide",
        officeName: "State Level Judge",
        districtName: "Montana",
      })
    ).toEqual({ kind: "supreme_court" });
    expect(
      toMontanaCersOfficeExpectation({
        officeScope: "statewide",
        officeName: "Public Service Commissioner",
        districtName: "Public Service Commission District 5",
      })
    ).toEqual({ kind: "psc", districtNumber: 5 });
    // Statewide-scope PSC elections sit on the numberless "Montana"
    // district; the seat number comes from the ballot title.
    expect(
      toMontanaCersOfficeExpectation({
        officeScope: "statewide",
        officeName: "Public Service Commissioner",
        districtName: "Montana",
        ballotTitle: "Public Service Commissioner, District 1",
      })
    ).toEqual({ kind: "psc", districtNumber: 1 });
    expect(
      toMontanaCersOfficeExpectation({
        officeScope: "statewide",
        officeName: "Public Service Commissioner",
        districtName: "Montana",
        ballotTitle: "Public Service Commissioner",
      })
    ).toEqual({ unmatchedReason: "missing_district_number" });
    // Missing district numbers fail closed instead of matching any district.
    expect(
      toMontanaCersOfficeExpectation({ ...SD43, legislativeDistrict: null })
    ).toEqual({ unmatchedReason: "missing_district_number" });
    expect(
      toMontanaCersOfficeExpectation({
        officeScope: "county",
        officeName: "Sheriff",
        districtName: "Ravalli County",
      })
    ).toEqual({ unmatchedReason: "unsupported_office" });
  });
});

describe("montanaCersOfficeTitleMatches", () => {
  it("matches the live-pinned CERS title shapes with padding tolerance", () => {
    expect(
      montanaCersOfficeTitleMatches({ kind: "legislative_upper", districtNumber: 43 }, "Senate District No. 43")
    ).toBe(true);
    expect(
      montanaCersOfficeTitleMatches({ kind: "legislative_upper", districtNumber: 43 }, "Senate District No. 44")
    ).toBe(false);
    expect(
      montanaCersOfficeTitleMatches({ kind: "legislative_lower", districtNumber: 7 }, "House District No. 07")
    ).toBe(true);
    // Legislative titles never cross chambers.
    expect(
      montanaCersOfficeTitleMatches({ kind: "legislative_upper", districtNumber: 7 }, "House District No. 7")
    ).toBe(false);
    // Supreme Court seat numbers are zero-padded and roster-invisible; any
    // seat within the judicial class matches.
    expect(montanaCersOfficeTitleMatches({ kind: "supreme_court" }, "Supreme Court Justice No. 04")).toBe(true);
    expect(montanaCersOfficeTitleMatches({ kind: "supreme_court" }, "Supreme Court Chief Justice")).toBe(true);
    expect(montanaCersOfficeTitleMatches({ kind: "supreme_court" }, "District Judge, District 4 Dept. 2")).toBe(
      false
    );
    expect(
      montanaCersOfficeTitleMatches({ kind: "psc", districtNumber: 1 }, "Public Service Commission District No. 1")
    ).toBe(true);
    expect(montanaCersOfficeTitleMatches({ kind: "supreme_court" }, null)).toBe(false);
  });
});

describe("resolveMontanaCersCandidate", () => {
  it("matches on full name + office title + year and stores the CERS display name", () => {
    const resolution = resolveMontanaCersCandidate({
      ...SD43,
      rows: [
        row({}),
        // Same office, different person — name evidence rejects it.
        row({ candidateId: 22000, lastName: "Wirth", firstName: "Zack", middleInitial: null }),
        // CERS test data never survives a roster-name match.
        row({ candidateId: 23000, lastName: "TEST", firstName: "Acct", officeTitle: "Supreme Court Justice No. 03" }),
      ],
    });
    expect(resolution).toMatchObject({
      status: "matched",
      cersCandidateId: 21020,
      cersCandidateName: "Bedey, David F.",
      confidence: "name_office_year_exact",
    });
  });

  it("rejects other election years and other districts", () => {
    expect(
      resolveMontanaCersCandidate({ ...SD43, rows: [row({ electionYear: 2024 })] })
    ).toMatchObject({ status: "unmatched", reason: "no_matching_cers_candidate" });
    expect(
      resolveMontanaCersCandidate({ ...SD43, rows: [row({ officeTitle: "Senate District No. 9" })] })
    ).toMatchObject({ status: "unmatched", reason: "no_matching_cers_candidate" });
  });

  it("reports ambiguity instead of guessing between two matching registrations", () => {
    const resolution = resolveMontanaCersCandidate({
      ...SD43,
      rows: [row({}), row({ candidateId: 21021 })],
    });
    expect(resolution).toMatchObject({ status: "ambiguous", reason: "multiple_matching_cers_candidates" });
  });

  it("never fuzzy-matches a different surname", () => {
    // The LYN BENNET / LYN BENNETT drift class: one letter off is a
    // different person until proven otherwise.
    expect(
      resolveMontanaCersCandidate({
        ...SD43,
        candidateName: "David Bedy",
        rows: [row({})],
      })
    ).toMatchObject({ status: "unmatched" });
  });
});

describe("montanaCersCandidateDisplayName", () => {
  it("renders Last, First Middle with graceful fallbacks", () => {
    expect(montanaCersCandidateDisplayName(row({}))).toBe("Bedey, David F.");
    expect(montanaCersCandidateDisplayName(row({ firstName: null, middleInitial: null }))).toBe("Bedey");
  });
});
