import { describe, expect, it } from "vitest";

import {
  normalizeConnecticutCandidateNameKeys,
  resolveConnecticutCandidateCommittee,
} from "../../../src/pipeline/connecticutFinance/connecticutCandidateCommitteeResolver.js";
import type { ConnecticutEcrisArtifactRow } from "../../../src/pipeline/connecticutFinance/connecticutEcrisArtifactReader.js";

function receiptRow(overrides: Partial<ConnecticutEcrisArtifactRow> = {}): ConnecticutEcrisArtifactRow {
  return {
    Committee: "ACKERT FOR THE 8TH",
    "Contributor Name": "Carolyn Gerrity",
    District: "8",
    "Office Sought": "State Representative",
    Employer: "RTX-Pratt Whitney",
    "Receipt Type": "Itemized Contributions from Individuals",
    "Committee Type": "Candidate Committee",
    "Transaction Date": "03/31/2026",
    "File To State": "04/01/2026",
    Amount: "50.00",
    "Receipt State": "Original",
    Occupation: "Business Manager",
    ElectionYear: "2026",
    "Committee ID": "14376",
    "Candidate First Name": "Timothy",
    "Candidate Middle Intial": "J",
    "Candidate Last Name": "Ackert",
    ...overrides,
  };
}

describe("connecticutCandidateCommitteeResolver", () => {
  it("normalizes candidate name keys without fuzzy matching", () => {
    expect([...normalizeConnecticutCandidateNameKeys("ACKERT, Timothy J.")]).toEqual([
      "ACKERT TIMOTHY J",
      "TIMOTHY J ACKERT",
      "TIMOTHY ACKERT",
    ]);
    expect([...normalizeConnecticutCandidateNameKeys("Timothy J. Ackert")]).toContain("TIMOTHY ACKERT");
  });

  it("matches a state representative committee by candidate, office, district, and election year", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy Ackert",
        officeName: "State Lower Chamber Legislator",
        district: "08",
        electionYear: 2026,
        sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
        receiptRows: [
          receiptRow(),
          receiptRow({ "Committee ID": "999", "Candidate Last Name": "Other" }),
          receiptRow({ "Committee ID": "888", "Committee Type": "Exploratory Committee" }),
          receiptRow({ "Committee ID": "777", ElectionYear: "2024" }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "14376",
      committeeName: "ACKERT FOR THE 8TH",
      confidence: "exact",
      source: "ecris_bulk",
      sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
      matchedReceiptRowCount: 1,
    });
  });

  it("matches a campaign nickname against the formal eCRIS filing name", () => {
    // VoteApp stores campaign names ("Joe Lauretti *") while eCRIS files
    // formal names; the VoteApp side expands nicknames, the row side stays
    // literal.
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Joe Lauretti *",
        officeName: "State Lower Chamber Legislator",
        district: "08",
        electionYear: 2026,
        sourceUrl: null,
        receiptRows: [
          receiptRow({
            Committee: "Lauretti for CT",
            "Candidate First Name": "Joseph",
            "Candidate Middle Intial": "",
            "Candidate Last Name": "Lauretti",
          }),
        ],
      })
    ).toMatchObject({ status: "matched", committeeName: "Lauretti for CT" });

    // Two distinct formal names must not meet at a shared nickname key.
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Patrick Smith",
        officeName: "State Lower Chamber Legislator",
        district: "08",
        electionYear: 2026,
        sourceUrl: null,
        receiptRows: [
          receiptRow({
            Committee: "Smith for CT",
            "Candidate First Name": "Patricia",
            "Candidate Middle Intial": "",
            "Candidate Last Name": "Smith",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched" });
  });

  it("refuses a shared-nickname input when both formal families filed", () => {
    // "Pat" expands to both Patrick and Patricia. With both filed for the
    // same office and district the resolver must not pick a side. When only
    // one family filed, linking it is deliberate: the same surname, office,
    // district, and election year already agree, and refusing would strand
    // real candidates (live-verified: Pat Callahan -> PATRICK, Sam Nestor ->
    // SAMANTHA).
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Pat Smith",
        officeName: "State Lower Chamber Legislator",
        district: "08",
        electionYear: 2026,
        sourceUrl: null,
        receiptRows: [
          receiptRow({
            Committee: "Patricia for CT",
            "Committee ID": "501",
            "Candidate First Name": "Patricia",
            "Candidate Middle Intial": "",
            "Candidate Last Name": "Smith",
          }),
          receiptRow({
            Committee: "Patrick for CT",
            "Committee ID": "502",
            "Candidate First Name": "Patrick",
            "Candidate Middle Intial": "",
            "Candidate Last Name": "Smith",
          }),
        ],
      })
    ).toMatchObject({ status: "ambiguous", reason: "multiple_matching_committees" });
  });

  it("matches a statewide office using Connecticut eCRIS source labels", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Jane Doe",
        officeName: "Secretary of State",
        electionYear: 2026,
        receiptRows: [
          receiptRow({
            Committee: "DOE FOR SECRETARY OF THE STATE",
            "Committee ID": "20001",
            District: "",
            "Office Sought": "Secretary of the State",
            "Candidate First Name": "Jane",
            "Candidate Middle Intial": "",
            "Candidate Last Name": "Doe",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "20001",
      committeeName: "DOE FOR SECRETARY OF THE STATE",
    });
  });

  it("does not match legislative rows without the election district", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy Ackert",
        officeName: "State Lower Chamber Legislator",
        electionYear: 2026,
        receiptRows: [receiptRow()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "TIMOTHY ACKERT",
      officeNameNormalized: "State Lower Chamber Legislator",
    });

    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy Ackert",
        officeName: "State Representative",
        electionYear: 2026,
        receiptRows: [receiptRow()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "TIMOTHY ACKERT",
      officeNameNormalized: "State Lower Chamber Legislator",
    });
  });

  it("returns ambiguous instead of guessing when multiple committees match", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy Ackert",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        receiptRows: [
          receiptRow(),
          receiptRow({
            Committee: "FRIENDS OF TIM ACKERT",
            "Committee ID": "99999",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "TIMOTHY ACKERT",
      officeNameNormalized: "State Lower Chamber Legislator",
      matches: [
        {
          committeeId: "14376",
          committeeName: "ACKERT FOR THE 8TH",
          confidence: "exact",
          source: "ecris_bulk",
          sourceUrl: null,
          matchedReceiptRowCount: 1,
        },
        {
          committeeId: "99999",
          committeeName: "FRIENDS OF TIM ACKERT",
          confidence: "exact",
          source: "ecris_bulk",
          sourceUrl: null,
          matchedReceiptRowCount: 1,
        },
      ],
    });
  });

  it("returns unmatched when candidate, office, district, or year do not line up", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Jennifer Ackert",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        receiptRows: [receiptRow()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_office_year_match" });

    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy Ackert",
        officeName: "State Senator",
        district: "8",
        electionYear: 2026,
        receiptRows: [receiptRow()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_office_year_match" });

    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy Ackert",
        officeName: "State Lower Chamber Legislator",
        district: "9",
        electionYear: 2026,
        receiptRows: [receiptRow()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_office_year_match" });

    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy Ackert",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2024,
        receiptRows: [receiptRow()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_office_year_match" });
  });

  it("rejects a same-race receipt row whose middle name contradicts the candidate", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy R. Ackert",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        receiptRows: [receiptRow()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_office_year_match" });
  });

  it("accepts an initial that corroborates the full middle name", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy James Ackert",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        receiptRows: [receiptRow()],
      })
    ).toMatchObject({ status: "matched", committeeId: "14376" });
  });

  it("still falls back to first+last when a side lacks middle info", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy J. Ackert",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        receiptRows: [receiptRow({ "Candidate Middle Intial": "" })],
      })
    ).toMatchObject({ status: "matched", committeeId: "14376" });
  });

  it("reads middle-name evidence through one-sided nickname expansion", () => {
    const rows = [
      receiptRow({
        "Candidate First Name": "Michael",
        "Candidate Middle Intial": "B",
        "Candidate Last Name": "Smith",
      }),
    ];

    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Mike A. Smith",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        receiptRows: rows,
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_office_year_match" });

    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Mike Smith",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        receiptRows: rows,
      })
    ).toMatchObject({ status: "matched", committeeId: "14376" });
  });

  it("rejects unsupported office names and invalid election years", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Jane Doe",
        officeName: "Judge of Probate",
        electionYear: 2026,
        receiptRows: [],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(() =>
      resolveConnecticutCandidateCommittee({
        candidateName: "Jane Doe",
        officeName: "Governor",
        electionYear: 2007,
        receiptRows: [],
      })
    ).toThrow("Invalid Connecticut candidate committee election year");
  });

  it("does not fuzzy-match candidate typos", () => {
    expect(
      resolveConnecticutCandidateCommittee({
        candidateName: "Timothy Ackertt",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        receiptRows: [receiptRow()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_office_year_match",
      candidateNameNormalized: "TIMOTHY ACKERTT",
      officeNameNormalized: "State Lower Chamber Legislator",
    });
  });
});
