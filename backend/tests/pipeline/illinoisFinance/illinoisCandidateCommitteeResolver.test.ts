import { describe, expect, it } from "vitest";

import {
  illinoisCandidateNameMiddleConflicts,
  normalizeIllinoisCandidateNameForStorage,
  normalizeIllinoisCandidateNameKeys,
  resolveIllinoisCandidateCommittee,
  resolveIllinoisCandidateCommitteesFromRelations,
  splitIllinoisCandidateNameForSearch,
} from "../../../src/pipeline/illinoisFinance/illinoisCandidateCommitteeResolver.js";
import type { IllinoisSbeContributionRecord } from "../../../src/pipeline/illinoisFinance/illinoisSbeClient.js";

function contribution(overrides: Partial<IllinoisSbeContributionRecord> = {}): IllinoisSbeContributionRecord {
  return {
    contributorName: "Pat Person",
    contributorAddress: "1 Main St",
    occupation: "Attorney",
    employer: "Law LLP",
    amount: 250,
    receivedDate: "3/1/2026",
    reportReceivedDate: null,
    contributionType: "Individual Contributions",
    recipientCommitteeName: "Friends of Jane Doe",
    description: null,
    vendorName: null,
    vendorAddress: null,
    sourceUrl: null,
    ...overrides,
  };
}

describe("illinoisCandidateCommitteeResolver", () => {
  it("normalizes candidate names for storage", () => {
    expect(normalizeIllinoisCandidateNameForStorage("Doe, Jane Q.")).toBe("DOE JANE Q");
    expect(normalizeIllinoisCandidateNameForStorage("Jane Doe (Janet Doe)")).toBe("JANE DOE");
  });

  it("resolves a single matching candidate committee from contribution records", () => {
    const resolution = resolveIllinoisCandidateCommittee({
      candidateName: "Jane Doe",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      contributionRecords: [
        contribution(),
        contribution({ receivedDate: "4/1/2025", amount: 500 }),
        contribution({ recipientCommitteeName: "Illinois Future PAC" }),
        contribution({ receivedDate: "12/31/2024" }),
      ],
      sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx",
    });

    expect(resolution).toMatchObject({
      status: "matched",
      matches: [
        {
          committeeKey: "FRIENDS OF JANE DOE",
          committeeName: "Friends of Jane Doe",
          confidence: "name_fallback",
          source: "illinois_sbe",
          sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx",
          matchedContributionRowCount: 2,
        },
      ],
    });
  });

  it("returns ambiguous when matching committees tie", () => {
    const resolution = resolveIllinoisCandidateCommittee({
      candidateName: "Jane Doe",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      contributionRecords: [
        contribution({ recipientCommitteeName: "Friends of Jane Doe" }),
        contribution({ recipientCommitteeName: "Jane Doe for Governor" }),
      ],
    });

    expect(resolution).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Governor",
      matches: [
        { committeeKey: "FRIENDS OF JANE DOE", matchedContributionRowCount: 1 },
        { committeeKey: "JANE DOE FOR GOVERNOR", matchedContributionRowCount: 1 },
      ],
    });
  });

  it("rejects a committee named for a middle-conflicting namesake", () => {
    // The token-subset test alone accepted this: candidate key "JANE DOE" is
    // a subset of {JANE, B, DOE}. The stripped committee text parses as a
    // person name, so the middle gate now rejects the other Jane Doe.
    expect(
      resolveIllinoisCandidateCommittee({
        candidateName: "Jane A. Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        contributionRecords: [contribution({ recipientCommitteeName: "Citizens for Jane B Doe" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("keeps a committee whose name corroborates the candidate's middle initial", () => {
    expect(
      resolveIllinoisCandidateCommittee({
        candidateName: "Jane A. Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        contributionRecords: [contribution({ recipientCommitteeName: "Friends of Jane Andrea Doe" })],
      })
    ).toMatchObject({
      status: "matched",
      matches: [{ committeeKey: "FRIENDS OF JANE ANDREA DOE" }],
    });
  });

  it("still falls back to first+last when the committee name lacks a middle", () => {
    expect(
      resolveIllinoisCandidateCommittee({
        candidateName: "Jane A. Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        contributionRecords: [contribution({ recipientCommitteeName: "Friends of Jane Doe" })],
      })
    ).toMatchObject({ status: "matched", matches: [{ committeeKey: "FRIENDS OF JANE DOE" }] });
  });

  it("keeps a middle-less committee name with trailing office tokens", () => {
    // The "Jane Doe" segment aligns weakly (no middle on the committee side),
    // so the token-subset verdict stands.
    expect(
      resolveIllinoisCandidateCommittee({
        candidateName: "Jane A. Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        contributionRecords: [contribution({ recipientCommitteeName: "Jane Doe for Attorney General" })],
      })
    ).toMatchObject({ status: "matched", matches: [{ committeeKey: "JANE DOE FOR ATTORNEY GENERAL" }] });
  });

  it("rejects middle-conflicting names hidden behind office or year designators", () => {
    // Trailing text used to block surname alignment, hiding the conflict
    // entirely; the designator expansion now isolates the person-name segment.
    for (const committeeName of [
      "Citizens for Jane B Doe for Governor",
      "Jane B Doe 2026",
      "Jane B Doe for Attorney General",
    ]) {
      expect(
        resolveIllinoisCandidateCommittee({
          candidateName: "Jane A. Doe",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2026,
          contributionRecords: [contribution({ recipientCommitteeName: committeeName })],
        })
      ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
    }
  });

  it("lets a corroborating middle survive the designator expansion", () => {
    expect(
      resolveIllinoisCandidateCommittee({
        candidateName: "Jane A. Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        contributionRecords: [contribution({ recipientCommitteeName: "Jane Andrea Doe for Governor" })],
      })
    ).toMatchObject({ status: "matched", matches: [{ committeeKey: "JANE ANDREA DOE FOR GOVERNOR" }] });
  });

  it("reads committee-name middle evidence through one-sided nickname expansion", () => {
    expect(
      resolveIllinoisCandidateCommittee({
        candidateName: "Mike A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        contributionRecords: [contribution({ recipientCommitteeName: "Citizens for Michael B Smith" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("requires a valid district for legislative office resolution", () => {
    expect(
      resolveIllinoisCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        electionYear: 2026,
        contributionRecords: [contribution()],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "missing_legislative_district",
    });
  });

  it("resolves every official committee relation for one local candidate", () => {
    const sourceUrl = "https://www.elections.il.gov/CampaignDisclosure/CandidateDetailCD.aspx?id=101";
    const resolution = resolveIllinoisCandidateCommitteesFromRelations({
      candidateName: "Jane Doe",
      officeScope: "place",
      officeName: "Mayor",
      electionYear: 2025,
      district: "Aurora city, Illinois",
      relations: [
        {
          candidateId: "101",
          candidateName: "Doe, Jane",
          electionYear: 2025,
          districtType: "City",
          district: "Aurora",
          office: "Mayor",
          isAtLarge: false,
          committeeId: "201",
          committeeName: "Aurora Forward",
          committeeStatus: "active",
          sourceUrl,
        },
        {
          candidateId: "101",
          candidateName: "Jane Doe",
          electionYear: 2025,
          districtType: "City",
          district: "Aurora",
          office: "Mayor",
          isAtLarge: false,
          committeeId: "202",
          committeeName: "Citizens for Aurora",
          committeeStatus: "final",
          sourceUrl,
        },
      ],
    });

    expect(resolution).toMatchObject({
      status: "matched",
      matches: [
        { committeeKey: "SBE:201", sbeCandidateId: "101", confidence: "official_relation" },
        { committeeKey: "SBE:202", sbeCandidateId: "101", confidence: "official_relation" },
      ],
    });
  });

  it("never bridges two distinct formal names through a shared nickname", () => {
    // Patrick and Patricia both shorten to Pat; the VoteApp side may expand,
    // but the SBE side stays literal, so PAT SMITH must not connect them.
    const patriciaRelation = {
      candidateId: "501",
      candidateName: "Patricia Smith",
      electionYear: 2026,
      districtType: "statewide",
      district: "Illinois",
      office: "Governor",
      isAtLarge: false,
      committeeId: "601",
      committeeName: "Citizens for Patricia Smith",
      committeeStatus: "active" as const,
      sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/DownloadCDDataFiles.aspx",
    };
    expect(
      resolveIllinoisCandidateCommitteesFromRelations({
        candidateName: "Patrick Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        relations: [patriciaRelation],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_official_candidate_relation" });

    // A genuinely ambiguous nickname input still refuses to pick a side.
    expect(
      resolveIllinoisCandidateCommitteesFromRelations({
        candidateName: "Pat Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        relations: [
          patriciaRelation,
          {
            ...patriciaRelation,
            candidateId: "502",
            candidateName: "Patrick Smith",
            committeeId: "602",
            committeeName: "Friends of Patrick Smith",
          },
        ],
      })
    ).toMatchObject({ status: "ambiguous", reason: "multiple_official_candidates" });
  });

  it("applies middle-name evidence to official candidate relations", () => {
    const smithRelation = {
      candidateId: "701",
      candidateName: "Smith, John B",
      electionYear: 2026,
      districtType: "statewide",
      district: "Illinois",
      office: "Governor",
      isAtLarge: false,
      committeeId: "801",
      committeeName: "Citizens for John Smith",
      committeeStatus: "active" as const,
      sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/DownloadCDDataFiles.aspx",
    };
    const resolve = (candidateName: string, candidateName2?: string) =>
      resolveIllinoisCandidateCommitteesFromRelations({
        candidateName,
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        relations: [
          candidateName2 ? { ...smithRelation, candidateName: candidateName2 } : smithRelation,
        ],
      });

    // A contradicting middle name rejects the relation.
    expect(resolve("John A. Smith")).toMatchObject({
      status: "unmatched",
      reason: "no_official_candidate_relation",
    });
    // An initial corroborating the full middle still matches.
    expect(resolve("John A. Smith", "Smith, John Andrew")).toMatchObject({ status: "matched" });
    // First+last still matches when a side lacks middle info.
    expect(resolve("John Smith")).toMatchObject({ status: "matched" });
  });

  it("treats a bare trailing V as a middle initial, not a generational suffix", () => {
    // Bare "V" is a middle initial, not a suffix (shared GENERATIONAL_SUFFIX_RANK
    // policy in finance/personNameMiddleEvidence.ts), so it must survive
    // normalization as middle evidence on either side of the comparison.
    expect(
      illinoisCandidateNameMiddleConflicts({ candidateName: "John V. Smith", rowNames: ["Smith, John B."] })
    ).toBe(true);
    expect(
      illinoisCandidateNameMiddleConflicts({ candidateName: "John B. Smith", rowNames: ["Smith, John V"] })
    ).toBe(true);
    expect(
      illinoisCandidateNameMiddleConflicts({ candidateName: "John V. Smith", rowNames: ["Smith, John V"] })
    ).toBe(false);
    expect(
      illinoisCandidateNameMiddleConflicts({ candidateName: "John Smith", rowNames: ["Smith, John V"] })
    ).toBe(false);
  });

  it("reads middle-name evidence through one-sided nickname expansion", () => {
    const relation = {
      candidateId: "901",
      candidateName: "Smith, Michael B",
      electionYear: 2026,
      districtType: "statewide",
      district: "Illinois",
      office: "Governor",
      isAtLarge: false,
      committeeId: "1001",
      committeeName: "Citizens for Michael Smith",
      committeeStatus: "active" as const,
      sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/DownloadCDDataFiles.aspx",
    };
    const resolve = (candidateName: string) =>
      resolveIllinoisCandidateCommitteesFromRelations({
        candidateName,
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        relations: [relation],
      });

    expect(resolve("Mike A. Smith")).toMatchObject({
      status: "unmatched",
      reason: "no_official_candidate_relation",
    });
    expect(resolve("Mike Smith")).toMatchObject({ status: "matched" });
  });

  it("splits search names around suffix-only comma segments", () => {
    expect(splitIllinoisCandidateNameForSearch("Curtis J Tarver, II")).toEqual({
      firstName: "Curtis J",
      lastName: "Tarver",
    });
    expect(splitIllinoisCandidateNameForSearch("Lawrence Walsh, Jr.")).toEqual({
      firstName: "Lawrence",
      lastName: "Walsh",
    });
    expect(splitIllinoisCandidateNameForSearch("Tarver, Curtis J")).toEqual({
      firstName: "Curtis J",
      lastName: "Tarver",
    });
    expect(splitIllinoisCandidateNameForSearch("II")).toBeNull();
  });

  it("keys a name whose comma introduces only a suffix like a plain name", () => {
    // "Curtis J Tarver, II" must still yield the middle-initial-free
    // "CURTIS TARVER" key; the suffix comma is not a Last, First flip.
    expect(normalizeIllinoisCandidateNameKeys("Curtis J Tarver, II")).toContain("CURTIS TARVER");
    // A genuine Last, First name still flips.
    expect(normalizeIllinoisCandidateNameKeys("Tarver, Curtis J")).toContain("CURTIS TARVER");
  });

  it("keys common nicknames against their formal first names only when asked", () => {
    const expand = { expandNicknames: true };
    expect(normalizeIllinoisCandidateNameKeys("Mike Frerichs", expand)).toContain("MICHAEL FRERICHS");
    expect(normalizeIllinoisCandidateNameKeys("Michael W Frerichs", expand)).toContain("MIKE FRERICHS");
    expect(normalizeIllinoisCandidateNameKeys("Frances Ann Hurley", expand)).toContain("FRAN HURLEY");
    // Unrelated first names must not gain variants.
    expect(normalizeIllinoisCandidateNameKeys("Zelda Frerichs", expand)).not.toContain("MICHAEL FRERICHS");
    // The default stays literal: source-side names never gain variant keys.
    expect(normalizeIllinoisCandidateNameKeys("Mike Frerichs")).not.toContain("MICHAEL FRERICHS");
  });

  it("resolves a nickname candidate against the formal SBE relation name", () => {
    const resolution = resolveIllinoisCandidateCommitteesFromRelations({
      candidateName: "Mike Frerichs",
      officeScope: "statewide",
      officeName: "State Treasurer",
      electionYear: 2026,
      relations: [
        {
          candidateId: "28183",
          candidateName: "Michael W Frerichs",
          electionYear: 2026,
          districtType: "statewide",
          district: "Illinois",
          office: "Treasurer",
          isAtLarge: false,
          committeeId: "23131",
          committeeName: "Friends of Frerichs",
          committeeStatus: "active",
          sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/DownloadCDDataFiles.aspx",
        },
      ],
    });

    expect(resolution).toMatchObject({
      status: "matched",
      matches: [{ committeeKey: "SBE:23131", sbeCandidateId: "28183" }],
    });
  });

  it("rejects cross-city and ward relations", () => {
    const base = {
      candidateId: "101",
      candidateName: "Jane Doe",
      electionYear: 2025,
      office: "Alderperson",
      isAtLarge: true,
      committeeId: "201",
      committeeName: "Aurora Forward",
      committeeStatus: "active" as const,
      sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/CandidateDetailCD.aspx?id=101",
    };
    expect(
      resolveIllinoisCandidateCommitteesFromRelations({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "Alderman",
        electionYear: 2025,
        district: "Aurora city, Illinois",
        relations: [{ ...base, districtType: "City", district: "Chicago" }],
      })
    ).toMatchObject({ status: "unmatched", reason: "jurisdiction_mismatch" });
    expect(
      resolveIllinoisCandidateCommitteesFromRelations({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "Alderman",
        electionYear: 2025,
        district: "Chicago city, Illinois",
        relations: [{ ...base, districtType: "Ward", district: "Chicago 44" }],
      })
    ).toMatchObject({ status: "unmatched", reason: "jurisdiction_mismatch" });
  });
});
