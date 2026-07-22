import { describe, expect, it, vi } from "vitest";

import { createKentuckyKrefCandidateFinanceLinkResolver } from "../../../src/pipeline/kentuckyFinance/kentuckyCandidateFinanceLinkResolver.js";
import type { KentuckyKrefContributionRecord } from "../../../src/pipeline/kentuckyFinance/kentuckyKrefClient.js";

function record(overrides: Partial<KentuckyKrefContributionRecord> = {}): KentuckyKrefContributionRecord {
  // Field shapes mirror a live KREF candidate-contribution export
  // (probe 2026-07-21: recipient = the candidate themselves, election tagged
  // to the specific primary/general date, office/location in display case).
  return {
    recipientName: "Adam Moore",
    candidateName: "Adam Moore",
    candidateFirstName: "Adam",
    candidateLastName: "Moore",
    office: "STATE REPRESENTATIVE",
    location: "45TH DISTRICT",
    electionDate: "5/19/2026",
    electionType: "PRIMARY",
    contributorName: "Pat Donor",
    contributorType: "INDIVIDUAL",
    contributionMode: "DIRECT",
    amount: 250,
    ...overrides,
  };
}

function candidateElection() {
  return {
    candidateId: "11111111-1111-1111-1111-111111111111",
    electionId: "22222222-2222-2222-2222-222222222222",
    candidateName: "Adam Moore",
    electionYear: 2026,
    electionDate: "2026-11-03",
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    location: "45",
  };
}

describe("kentuckyCandidateFinanceLinkResolver", () => {
  it("matches a candidate across primary/general dates within the election year", async () => {
    const downloadCandidateContributions = vi.fn().mockResolvedValue([
      record(),
      record({ electionDate: "11/3/2026", electionType: "GENERAL", amount: 100 }),
      // Prior-cycle rows for the same candidate must not affect resolution.
      record({ electionDate: "11/5/2024", amount: 50 }),
    ]);
    const resolver = createKentuckyKrefCandidateFinanceLinkResolver({
      krefClient: { downloadCandidateContributions },
    });

    const resolution = await resolver(candidateElection());

    expect(resolution).toEqual({
      status: "matched",
      candidateKey: "adam moore|state lower chamber legislator|state lower|2026-11-03",
      committeeKey: "adam moore",
      committeeName: "Adam Moore",
      sourceUrl: expect.stringContaining("ExportContributors"),
    });
    expect(downloadCandidateContributions).toHaveBeenCalledWith(
      { candidateFirstName: "Adam", candidateLastName: "Moore" },
      undefined
    );
  });

  it("does not match when only prior-cycle rows exist", async () => {
    const downloadCandidateContributions = vi
      .fn()
      .mockResolvedValue([record({ electionDate: "11/5/2024" }), record({ electionDate: "5/21/2024" })]);
    const resolver = createKentuckyKrefCandidateFinanceLinkResolver({
      krefClient: { downloadCandidateContributions },
    });

    await expect(resolver(candidateElection())).resolves.toEqual({
      status: "unmatched",
      reason: "no_kref_contribution_match",
    });
  });

  it("does not match rows from another district", async () => {
    const downloadCandidateContributions = vi.fn().mockResolvedValue([record({ location: "44TH DISTRICT" })]);
    const resolver = createKentuckyKrefCandidateFinanceLinkResolver({
      krefClient: { downloadCandidateContributions },
    });

    await expect(resolver(candidateElection())).resolves.toEqual({
      status: "unmatched",
      reason: "no_kref_contribution_match",
    });
  });

  it("refuses to guess between multiple committee identities", async () => {
    const downloadCandidateContributions = vi
      .fn()
      .mockResolvedValue([record(), record({ recipientName: "Friends of Adam Moore" })]);
    const resolver = createKentuckyKrefCandidateFinanceLinkResolver({
      krefClient: { downloadCandidateContributions },
    });

    await expect(resolver(candidateElection())).resolves.toEqual({
      status: "ambiguous",
      reason: "multiple_kref_committee_identities",
      matchCount: 2,
    });
  });

  it("falls back to the candidate name when recipient names are blank", async () => {
    const downloadCandidateContributions = vi
      .fn()
      .mockResolvedValue([record({ recipientName: "", toOrganizationName: "" })]);
    const resolver = createKentuckyKrefCandidateFinanceLinkResolver({
      krefClient: { downloadCandidateContributions },
    });

    await expect(resolver(candidateElection())).resolves.toMatchObject({
      status: "matched",
      committeeKey: "adam moore",
      committeeName: "Adam Moore",
    });
  });
});
