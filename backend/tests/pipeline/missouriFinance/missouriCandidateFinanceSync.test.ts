import { describe, expect, it, vi } from "vitest";

import { resolveMissouriCandidateFinanceCycleWindow, syncMissouriCandidateFinance } from "../../../src/pipeline/missouriFinance/missouriCandidateFinanceSync.js";
import type { MissouriCandidateFinanceArtifacts } from "../../../src/pipeline/missouriFinance/missouriCandidateFinanceSync.js";

const artifacts: MissouriCandidateFinanceArtifacts = {
  committeeInfo: {
    mecid: "C263985", committeeName: "Jane for Missouri", candidateName: "Jane Doe",
    electionHistory: [
      { electionDate: "2026-11-03", electionType: "General Election", office: "State Representative", politicalSubdivision: "Missouri House" },
      { electionDate: "2026-08-04", electionType: "Primary Election", office: "State Representative", politicalSubdivision: "Missouri House" },
    ],
    sourceUrl: "https://example.test/committee",
  },
  inventory: [{ reportId: "1", report: "8 Day Before General Election", dateFiled: "2026-10-26", isAmended: false, lineageKey: "8 DAY BEFORE GENERAL ELECTION" }],
  contributionRows: [{
    mecid: "C263985", committeeName: "Jane for Missouri", report: "8 Day Before General Election",
    contributorCommittee: null, contributorCompany: null, contributorLastName: "Doe", contributorFirstName: "John",
    employer: null, occupation: "Teacher", contributionDate: "2026-09-01", amountCents: 12500, contributionKind: "Monetary",
  }],
  expenditureRows: [{
    mecid: "C263985", committeeName: "Jane for Missouri", report: "8 Day Before General Election",
    payeeLastName: null, payeeFirstName: null, payeeCompany: "Printer", purpose: "Signs",
    expenditureDate: "2026-10-01", amountCents: 5000, expenditureType: "Paid",
  }],
  contributionSourceUrl: "https://example.test/contributions",
  expenditureSourceUrl: "https://example.test/expenditures",
};

describe("missouriCandidateFinanceSync", () => {
  it("starts the general cycle the day after the same-year primary", () => {
    expect(resolveMissouriCandidateFinanceCycleWindow({ electionDate: "2026-11-03", committeeInfo: artifacts.committeeInfo })).toEqual({
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03", primaryElectionDate: "2026-08-04",
    });
  });

  it("fails closed when MEC history cannot prove the primary/general boundary", () => {
    expect(() => resolveMissouriCandidateFinanceCycleWindow({
      electionDate: "2026-11-03",
      committeeInfo: { ...artifacts.committeeInfo, electionHistory: artifacts.committeeInfo.electionHistory.slice(0, 1) },
    })).toThrow("no same-year primary boundary");
  });

  it("builds a dry-run direct snapshot from cache data without DB writes", async () => {
    const query = vi.fn();
    const result = await syncMissouriCandidateFinance({
      db: { query, connect: vi.fn() } as never,
      candidateId: "11111111-1111-4111-8111-111111111111",
      electionId: "22222222-2222-4222-8222-222222222222",
      candidateName: "Jane Doe", electionYear: 2026, electionDate: "2026-11-03",
      officeName: "State Lower Chamber Legislator", district: "1",
      committee: { committeeId: "C263985", committeeName: "Jane for Missouri", linkSource: "mec_portal" },
      artifacts, dryRun: true, now: new Date("2026-08-19T00:00:00Z"),
    });
    expect(result).toMatchObject({ dryRun: true, cycleStart: "2026-08-05", cycleEnd: "2026-11-03", summaryWritten: false });
    expect(result.aggregation).toMatchObject({ directContributionTotal: 125, totalDisbursements: 50 });
    expect(query).not.toHaveBeenCalled();
  });

  it("refuses to publish a partial snapshot when an in-cycle amendment is ambiguous", async () => {
    const april = "April Quarterly Report";
    const ambiguous: MissouriCandidateFinanceArtifacts = {
      ...artifacts,
      inventory: [
        { reportId: "1", report: april, dateFiled: "2026-09-01", isAmended: false, lineageKey: april.toUpperCase() },
        { reportId: "2", report: `AMENDED ${april}`, dateFiled: "2026-09-02", isAmended: true, lineageKey: april.toUpperCase() },
      ],
      contributionRows: [
        { ...artifacts.contributionRows[0]!, report: april, amountCents: 30000 },
        { ...artifacts.contributionRows[0]!, report: `AMENDED ${april}`, amountCents: 12500, contributionKind: "In-Kind" },
      ],
      expenditureRows: [],
    };
    const query = vi.fn();
    await expect(syncMissouriCandidateFinance({
      db: { query, connect: vi.fn() } as never,
      candidateId: "11111111-1111-4111-8111-111111111111", electionId: "22222222-2222-4222-8222-222222222222",
      candidateName: "Jane Doe", electionYear: 2026, electionDate: "2026-11-03", officeName: "State Lower Chamber Legislator",
      committee: { committeeId: "C263985", committeeName: "Jane for Missouri", linkSource: "mec_portal" },
      artifacts: ambiguous,
    })).rejects.toThrow("in-cycle report lineage is not publishable");
    expect(query).not.toHaveBeenCalled();
  });
});
