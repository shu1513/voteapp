import { describe, expect, it } from "vitest";

import {
  normalizeIllinoisCandidateNameForStorage,
  resolveIllinoisCandidateCommittee,
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

    expect(resolution).toEqual({
      status: "matched",
      committeeKey: "JANE DOE",
      committeeName: "Friends of Jane Doe",
      confidence: "exact",
      source: "illinois_sbe",
      sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx",
      matchedContributionRowCount: 2,
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
        { committeeKey: "JANE DOE", matchedContributionRowCount: 1 },
        { committeeKey: "JANE DOE GOVERNOR", matchedContributionRowCount: 1 },
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
});
