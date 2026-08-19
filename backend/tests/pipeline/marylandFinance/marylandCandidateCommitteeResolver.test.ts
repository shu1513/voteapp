import { describe, expect, it } from "vitest";

import {
  normalizeMarylandCandidateNameKeys,
  resolveMarylandCandidateCommittee,
} from "../../../src/pipeline/marylandFinance/marylandCandidateCommitteeResolver.js";
import {
  MARYLAND_CFS_COMMITTEE_COLUMNS,
  type MarylandCfsCommitteeRow,
} from "../../../src/pipeline/marylandFinance/marylandCfsArtifactReader.js";

function committee(overrides: Partial<MarylandCfsCommitteeRow> = {}): MarylandCfsCommitteeRow {
  const row = Object.fromEntries(MARYLAND_CFS_COMMITTEE_COLUMNS.map((column) => [column, ""])) as MarylandCfsCommitteeRow;
  return {
    ...row,
    "Filing Entity Id": "16018290",
    "Committee Name": "Gallucci, Justin Friends of",
    "Committee Type": "Candidate Committee",
    Election: "Gubernatorial - 11/08/2026",
    "Candidate LastName": "Gallucci",
    "Candidate First Name": "Justin",
    "Candidate Middle Name": "",
    "Candidate Suffix": "",
    Jurisdiction: "Maryland State",
    "Office Sought": "State Senator",
    "Party Affiliation": "Republican",
    ...overrides,
  };
}

describe("marylandCandidateCommitteeResolver", () => {
  it("normalizes direct, comma-form, and parenthetical candidate names without fuzzy matching", () => {
    expect([...normalizeMarylandCandidateNameKeys("GALLUCCI, Justin L.")]).toEqual([
      "GALLUCCI JUSTIN L",
      "JUSTIN L GALLUCCI",
      "JUSTIN GALLUCCI",
    ]);
    expect([...normalizeMarylandCandidateNameKeys("William (Bill) Holtzinger")]).toContain("BILL HOLTZINGER");
  });

  it("matches exactly one Maryland candidate committee by candidate, office, and election year", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        sourceUrl: "https://campaignfinance.maryland.gov/public/cf/downloads",
        committeeRows: [
          committee({ "Office Sought": "Governor/Lieutenant Governor" }),
          committee({
            "Filing Entity Id": "999",
            "Committee Name": "Other Candidate",
            "Candidate LastName": "Other",
            "Candidate First Name": "Person",
          }),
          committee({
            "Filing Entity Id": "888",
            "Committee Type": "Political Action Committee (PAC)",
          }),
          committee({
            "Filing Entity Id": "777",
            Election: "Gubernatorial - 11/08/2022",
          }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "16018290",
      committeeName: "Gallucci, Justin Friends of",
      confidence: "exact",
      source: "cfs_public_export",
      sourceUrl: "https://campaignfinance.maryland.gov/public/cf/downloads",
      matchedCommitteeRowCount: 1,
    });
  });

  it("matches safe Maryland source-like office labels", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Dan Cox",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Filing Entity Id": "16018291",
            "Committee Name": "Dan Cox for Frederick",
            "Candidate LastName": "Cox",
            "Candidate First Name": "Daniel",
            "Office Sought": "Governor/Lieutenant Governor",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "16018291",
    });

    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Brooke Lierman",
        officeScope: "statewide",
        officeName: "State Comptroller",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Filing Entity Id": "2001",
            "Committee Name": "Lierman for Maryland",
            "Candidate LastName": "Lierman",
            "Candidate First Name": "Brooke",
            "Office Sought": "Comptroller",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "2001",
    });

    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Yonelle Moore Lee",
        officeScope: "state_lower",
        officeName: "House of Delegates",
        district: "25",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Filing Entity Id": "16017233",
            "Committee Name": "Friends of Yonelle Moore Lee",
            "Candidate LastName": "MOORE LEE",
            "Candidate First Name": "YONELLE",
            "Office Sought": "House of Delegates",
          }),
        ],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "unverified_legislative_district",
    });
  });

  it("rejects a same-race committee whose middle name contradicts the candidate", () => {
    // Same office and election year — only the middle evidence differs.
    // Without the middle gate this committee linked as an "exact" match and
    // attached the other Justin Gallucci's finance records.
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin L. Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Office Sought": "Governor",
            "Candidate Middle Name": "M.",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("accepts an initial that corroborates the full middle name", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin L. Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Office Sought": "Governor",
            "Candidate Middle Name": "Lee",
          }),
        ],
      })
    ).toMatchObject({ status: "matched", committeeId: "16018290" });
  });

  it("still falls back to first+last when a side lacks middle info", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Office Sought": "Governor",
            "Candidate Middle Name": "M.",
          }),
        ],
      })
    ).toMatchObject({ status: "matched", committeeId: "16018290" });
  });

  it("treats a bare trailing V as a middle initial, not a generational suffix", () => {
    // Bare "V" is a middle initial, not a suffix (shared GENERATIONAL_SUFFIX_RANK
    // policy in finance/personNameMiddleEvidence.ts), so it must survive
    // normalization as middle evidence on either side of the comparison.
    const resolve = (candidateName: string, rowMiddleName: string) =>
      resolveMarylandCandidateCommittee({
        candidateName,
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Office Sought": "Governor",
            "Candidate Middle Name": rowMiddleName,
          }),
        ],
      });

    expect(resolve("Justin V. Gallucci", "M.")).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
    expect(resolve("Justin M. Gallucci", "V")).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
    expect(resolve("Justin V. Gallucci", "V")).toMatchObject({ status: "matched", committeeId: "16018290" });
    expect(resolve("Justin Gallucci", "V")).toMatchObject({ status: "matched", committeeId: "16018290" });
  });

  it("lets a middle conflict veto a committee-name match on the same row", () => {
    // The committee name carries the middle-less "Friends of Justin Gallucci"
    // alias, but the row's own candidate fields name a different person.
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin L. Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Office Sought": "Governor",
            "Committee Name": "Friends of Justin Gallucci",
            "Candidate Middle Name": "M.",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("treats public financing committees with candidate fields as candidate committees", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Robert Cockey",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Filing Entity Id": "16017221",
            "Committee Name": "Cockey, Bob for HOCO",
            "Committee Type": "Public Financing Committee",
            "Candidate LastName": "Cockey",
            "Candidate First Name": "Robert",
            "Office Sought": "Governor/Lieutenant Governor",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "16017221",
    });
  });

  it("requires districts for legislative offices because TCMD rows do not prove district", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2026,
        committeeRows: [committee()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "JUSTIN GALLUCCI",
      officeNameNormalized: "State Senator",
    });

    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "33",
        electionYear: 2026,
        committeeRows: [committee()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "unverified_legislative_district",
      candidateNameNormalized: "JUSTIN GALLUCCI",
      officeNameNormalized: "State Senator",
    });
  });

  it("does not guess when multiple Maryland candidate committees match", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({ "Office Sought": "Governor/Lieutenant Governor" }),
          committee({
            "Filing Entity Id": "16018291",
            "Committee Name": "Friends of Justin Gallucci",
            "Office Sought": "Governor/Lieutenant Governor",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JUSTIN GALLUCCI",
      officeNameNormalized: "Governor",
      matches: [
        {
          committeeId: "16018290",
          committeeName: "Gallucci, Justin Friends of",
          confidence: "exact",
          source: "cfs_public_export",
          sourceUrl: null,
          matchedCommitteeRowCount: 1,
        },
        {
          committeeId: "16018291",
          committeeName: "Friends of Justin Gallucci",
          confidence: "exact",
          source: "cfs_public_export",
          sourceUrl: null,
          matchedCommitteeRowCount: 1,
        },
      ],
    });
  });

  it("returns unmatched for unsupported offices or missing names", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "county",
        officeName: "County Executive",
        electionYear: 2026,
        committeeRows: [committee()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "COUNTY EXECUTIVE",
    });

    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [committee()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Governor",
    });
  });

  it("returns unmatched when candidate, committee type, office, jurisdiction, or election year does not match", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Committee Name": "Friends of Janet Gallucci",
            "Candidate First Name": "Janet",
            "Office Sought": "Governor/Lieutenant Governor",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            "Committee Type": "Super Political Action Committee (Super PAC)",
            "Office Sought": "Governor/Lieutenant Governor",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [committee({ "Office Sought": "House of Delegates" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [committee({ Jurisdiction: "Frederick", "Office Sought": "Governor/Lieutenant Governor" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [
          committee({
            Election: "Gubernatorial - 11/08/2022",
            "Election Year": "2022",
            "Office Sought": "Governor/Lieutenant Governor",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("does not fuzzy-match candidate typos", () => {
    expect(
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Galluccii",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committeeRows: [committee({ "Office Sought": "Governor/Lieutenant Governor" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: "JUSTIN GALLUCCII",
      officeNameNormalized: "Governor",
    });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveMarylandCandidateCommittee({
        candidateName: "Justin Gallucci",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "33",
        electionYear: 1999,
        committeeRows: [],
      })
    ).toThrow("Invalid Maryland candidate committee election year");
  });
});
