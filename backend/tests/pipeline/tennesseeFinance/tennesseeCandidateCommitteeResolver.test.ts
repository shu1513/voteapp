import { describe, expect, it } from "vitest";

import {
  normalizeTennesseeCandidateNameKeys,
  resolveTennesseeCandidateCommittee,
} from "../../../src/pipeline/tennesseeFinance/tennesseeCandidateCommitteeResolver.js";
import type { TennesseeCampCandidateRecord } from "../../../src/pipeline/tennesseeFinance/tennesseeCampClient.js";

function record(overrides: Partial<TennesseeCampCandidateRecord> = {}): TennesseeCampCandidateRecord {
  return {
    campCandidateId: "6496",
    ownerName: "LEE, BILL",
    name: "LEE, BILL",
    officeSought: "Governor",
    district: null,
    electionYear: 2022,
    reportListUrl: "https://apps.tn.gov/tncamp/public/replist.htm?id=6496&owner=LEE%2C+BILL",
    sourceUrl: "https://apps.tn.gov/tncamp/public/cpsearch.htm",
    ...overrides,
  };
}

describe("tennesseeCandidateCommitteeResolver", () => {
  it("normalizes comma-form and parenthetical Tennessee candidate names", () => {
    expect([...normalizeTennesseeCandidateNameKeys("LEE, Bill (Bill Lee)")]).toEqual(["LEE BILL", "BILL LEE"]);
  });

  it("matches exactly one Tennessee CAMP candidate by name, office, year, and district", () => {
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "Bill Lee",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        candidateRecords: [record(), record({ campCandidateId: "other", name: "LEE, REBECCA V.", officeSought: "Public Defender" })],
      })
    ).toEqual({
      status: "matched",
      campCandidateId: "6496",
      ownerName: "LEE, BILL",
      candidateName: "LEE, BILL",
      officeSought: "Governor",
      district: null,
      confidence: "exact",
      source: "tncamp_search",
      sourceUrl: "https://apps.tn.gov/tncamp/public/cpsearch.htm",
      reportListUrl: "https://apps.tn.gov/tncamp/public/replist.htm?id=6496&owner=LEE%2C+BILL",
      matchedRowCount: 1,
    });
  });

  it("requires legislative districts and matches zero-padded CAMP districts", () => {
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2026,
        district: "4",
        candidateRecords: [
          record({
            campCandidateId: "1",
            ownerName: "DOE, JANE",
            name: "DOE, JANE",
            officeSought: "Senate",
            district: "04",
            electionYear: 2026,
          }),
        ],
      })
    ).toMatchObject({ status: "matched", campCandidateId: "1" });

    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2026,
        candidateRecords: [],
      })
    ).toMatchObject({ status: "unmatched", reason: "missing_legislative_district" });
  });

  it("rejects a same-race record whose middle name contradicts the candidate", () => {
    // Same office and year — only the middle evidence differs. Without the
    // middle gate this record linked as an "exact" match and attached the
    // other John Smith's finance records.
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "John A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({ campCandidateId: "9", ownerName: "SMITH, JOHN B.", name: "SMITH, JOHN B.", electionYear: 2026 }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("accepts an initial that corroborates the full middle name", () => {
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "John A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({
            campCandidateId: "9",
            ownerName: "SMITH, JOHN ANDREW",
            name: "SMITH, JOHN ANDREW",
            electionYear: 2026,
          }),
        ],
      })
    ).toMatchObject({ status: "matched", campCandidateId: "9" });
  });

  it("still falls back to first+last when a side lacks middle info", () => {
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "John Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({ campCandidateId: "9", ownerName: "SMITH, JOHN B.", name: "SMITH, JOHN B.", electionYear: 2026 }),
        ],
      })
    ).toMatchObject({ status: "matched", campCandidateId: "9" });
  });

  it("lets a middle conflict veto a middle-less sibling name on the same record", () => {
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "John A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({ campCandidateId: "9", ownerName: "SMITH, JOHN B.", name: "JOHN SMITH", electionYear: 2026 }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("treats a bare V as a middle initial, not a generational suffix", () => {
    // Shared policy (GENERATIONAL_SUFFIX_RANK in finance/personNameMiddleEvidence.ts)
    // deliberately excludes "V": it is far more often a middle initial than a
    // fifth-generation suffix, so it must stay as middle evidence on either side.
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "John V. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({ campCandidateId: "9", ownerName: "SMITH, JOHN B.", name: "SMITH, JOHN B.", electionYear: 2026 }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "John B. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({ campCandidateId: "9", ownerName: "SMITH, JOHN V", name: "SMITH, JOHN V", electionYear: 2026 }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "John V. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({ campCandidateId: "9", ownerName: "SMITH, JOHN V", name: "SMITH, JOHN V", electionYear: 2026 }),
        ],
      })
    ).toMatchObject({ status: "matched", campCandidateId: "9" });

    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "John Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({ campCandidateId: "9", ownerName: "SMITH, JOHN V", name: "SMITH, JOHN V", electionYear: 2026 }),
        ],
      })
    ).toMatchObject({ status: "matched", campCandidateId: "9" });
  });

  it("does not guess when multiple CAMP candidates match", () => {
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [
          record({ campCandidateId: "a", ownerName: "DOE, JANE", name: "DOE, JANE", electionYear: 2026 }),
          record({ campCandidateId: "b", ownerName: "DOE, JANE", name: "DOE, JANE", electionYear: 2026 }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      matches: [{ campCandidateId: "a" }, { campCandidateId: "b" }],
    });
  });

  it("returns unmatched for unsupported offices and missing names", () => {
    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "judicial",
        officeName: "Supreme Court",
        electionYear: 2026,
        candidateRecords: [],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveTennesseeCandidateCommittee({
        candidateName: " ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRecords: [],
      })
    ).toMatchObject({ status: "unmatched", reason: "missing_candidate_name" });
  });
});
