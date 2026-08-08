import { describe, expect, it } from "vitest";

import {
  normalizeColoradoCandidateNameKeys,
  resolveColoradoCandidateCommittee,
} from "../../../src/pipeline/coloradoFinance/coloradoCandidateCommitteeResolver.js";
import type { ColoradoTracerContributionRow } from "../../../src/pipeline/coloradoFinance/coloradoTracerContributionReader.js";

function contribution(overrides: Partial<ColoradoTracerContributionRow> = {}): ColoradoTracerContributionRow {
  return {
    CO_ID: "202650001",
    ContributionAmount: "100.00",
    ContributionDate: "01/10/2026",
    LastName: "Doe",
    FirstName: "Jane",
    MI: "",
    Suffix: "",
    Address1: "",
    Address2: "",
    City: "Denver",
    State: "CO",
    Zip: "80203",
    Explanation: "",
    RecordID: "R1",
    FiledDate: "02/01/2026",
    ContributionType: "Monetary",
    ReceiptType: "Contribution",
    ContributorType: "Individual",
    Electioneering: "",
    CommitteeType: "Candidate Committee",
    CommitteeName: "Jane Doe for Colorado Governor",
    CandidateName: "Jane Doe",
    Employer: "Acme Inc",
    Occupation: "Engineer",
    Amended: "False",
    Amendment: "",
    AmendedRecordID: "",
    Jurisdiction: "STATEWIDE",
    OccupationComments: "",
    ...overrides,
  };
}

describe("coloradoCandidateCommitteeResolver", () => {
  it("normalizes direct and comma-form candidate names", () => {
    expect([...normalizeColoradoCandidateNameKeys("DOE, Jane Q.")]).toEqual([
      "DOE JANE Q",
      "JANE Q DOE",
    ]);
  });

  it("matches exactly one candidate committee for the candidate and cycle", () => {
    expect(
      resolveColoradoCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        sourceUrl: "https://tracer.sos.colorado.gov/",
        contributionRows: [
          contribution({ CandidateName: "DOE, JANE" }),
          contribution({ CO_ID: "999", CandidateName: "Other Person" }),
          contribution({ CO_ID: "888", CandidateName: "Jane Doe", CommitteeType: "Issue Committee" }),
          contribution({ CO_ID: "777", CandidateName: "Jane Doe", ContributionDate: "01/10/2024" }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "202650001",
      committeeName: "Jane Doe for Colorado Governor",
      sourceUrl: "https://tracer.sos.colorado.gov/",
    });
  });

  it("does not guess when multiple candidate committees match", () => {
    expect(
      resolveColoradoCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        contributionRows: [
          contribution(),
          contribution({
            CO_ID: "202650002",
            CommitteeName: "Coloradans for Jane Doe",
            CandidateName: "Jane Doe",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
    });
  });

  it("recovers a row when only the candidate side carries a middle", () => {
    // Full-string keys never overlap here ("JANE Q DOE" vs "JANE DOE"), which
    // silently stranded the link before the middle-evidence fallback.
    expect(
      resolveColoradoCandidateCommittee({
        candidateName: "Jane Q. Doe",
        electionYear: 2026,
        contributionRows: [contribution()],
      })
    ).toMatchObject({ status: "matched", committeeId: "202650001" });
  });

  it("recovers a row when only the TRACER side carries a middle", () => {
    expect(
      resolveColoradoCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        contributionRows: [contribution({ CandidateName: "Doe, Jane Q." })],
      })
    ).toMatchObject({ status: "matched", committeeId: "202650001" });
  });

  it("still refuses a row whose middle name contradicts the candidate", () => {
    expect(
      resolveColoradoCandidateCommittee({
        candidateName: "Jane Q. Doe",
        electionYear: 2026,
        contributionRows: [contribution({ CandidateName: "Doe, Jane R." })],
      })
    ).toEqual({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("returns unmatched when there is no candidate committee match", () => {
    expect(
      resolveColoradoCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        contributionRows: [contribution({ CandidateName: "Other Person" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
  });
});
