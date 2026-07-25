import { describe, expect, it } from "vitest";

import {
  normalizeIllinoisCandidateNameForStorage,
  normalizeIllinoisCandidateNameKeys,
  resolveIllinoisCandidateCommittee,
  resolveIllinoisCandidateCommitteesFromRelations,
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

  it("keys a name whose comma introduces only a suffix like a plain name", () => {
    // "Curtis J Tarver, II" must still yield the middle-initial-free
    // "CURTIS TARVER" key; the suffix comma is not a Last, First flip.
    expect(normalizeIllinoisCandidateNameKeys("Curtis J Tarver, II")).toContain("CURTIS TARVER");
    // A genuine Last, First name still flips.
    expect(normalizeIllinoisCandidateNameKeys("Tarver, Curtis J")).toContain("CURTIS TARVER");
  });

  it("keys common nicknames against their formal first names", () => {
    expect(normalizeIllinoisCandidateNameKeys("Mike Frerichs")).toContain("MICHAEL FRERICHS");
    expect(normalizeIllinoisCandidateNameKeys("Michael W Frerichs")).toContain("MIKE FRERICHS");
    expect(normalizeIllinoisCandidateNameKeys("Frances Ann Hurley")).toContain("FRAN HURLEY");
    // Unrelated first names must not gain variants.
    expect(normalizeIllinoisCandidateNameKeys("Zelda Frerichs")).not.toContain("MICHAEL FRERICHS");
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
