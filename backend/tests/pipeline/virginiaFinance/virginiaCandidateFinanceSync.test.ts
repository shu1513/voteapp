import { describe, expect, it, vi } from "vitest";

import type { VirginiaScheduleAContribution } from "../../../src/pipeline/virginiaFinance/virginiaCampaignFinanceClient.js";
import { syncVirginiaCandidateFinance } from "../../../src/pipeline/virginiaFinance/virginiaCandidateFinanceSync.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function contribution(input: Partial<VirginiaScheduleAContribution>): VirginiaScheduleAContribution {
  return {
    contributorName: input.contributorName ?? "Contributor",
    isIndividual: input.isIndividual ?? true,
    employer: input.employer ?? null,
    occupationOrTypeOfBusiness: input.occupationOrTypeOfBusiness ?? "Attorney",
    transactionDate: input.transactionDate ?? "10/01/2025",
    amount: input.amount ?? 100,
    totalToDate: input.totalToDate ?? null,
  };
}

function baseInput(overrides: Partial<Parameters<typeof syncVirginiaCandidateFinance>[0]> = {}) {
  return {
    db: createMockDb(),
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Jane Doe",
    electionYear: 2025,
    officeName: "Governor",
    district: null,
    committeeId: "committee-123",
    committeeCode: "CC-123",
    committeeName: "Jane Doe for Governor",
    sourceUrl: "https://cfreports.elections.virginia.gov/Committee/Index/committee-123",
    contributionSourceUrl: "https://cfreports.elections.virginia.gov/Report/Xml/123",
    now: new Date("2025-02-03T04:05:06.000Z"),
    contributions: [
      contribution({
        contributorName: "Alice Voter",
        occupationOrTypeOfBusiness: "Attorney",
        amount: 250,
      }),
      contribution({
        contributorName: "Bob Voter",
        occupationOrTypeOfBusiness: "Teacher",
        amount: 125,
      }),
      contribution({
        contributorName: "PAC Row",
        isIndividual: false,
        occupationOrTypeOfBusiness: "Political Committee",
        amount: 1000,
      }),
    ],
    ...overrides,
  };
}

describe("virginiaCandidateFinanceSync", () => {
  it("aggregates individual Schedule A contributions and writes a Virginia snapshot", async () => {
    const input = baseInput();

    const result = await syncVirginiaCandidateFinance(input);

    expect(result).toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2025,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 4,
      totalReceipts: 375,
      directContributionTotal: 375,
      matchedContributionRowCount: 3,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 1,
    });

    const sql = input.db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.va_candidate_finance_links"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.va_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.va_candidate_finance_direct_breakdowns"))).toHaveLength(4);
    expect(input.db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2025,
      "JANE DOE",
      "Governor",
      null,
      "committee-123",
      "CC-123",
      "Jane Doe for Governor",
      "active",
      "manual",
      "https://cfreports.elections.virginia.gov/Committee/Index/committee-123",
      "2025-02-03T04:05:06.000Z",
    ]);
  });

  it("does not write in dry-run mode but returns aggregation counts", async () => {
    const input = baseInput({
      db: createMockDb(),
      dryRun: true,
    });

    const result = await syncVirginiaCandidateFinance(input);

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: 375,
      directContributionTotal: 375,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 1,
    });
    expect(input.db.query).not.toHaveBeenCalled();
  });

  it("uses the contribution source URL for stored direct breakdowns", async () => {
    const input = baseInput();

    await syncVirginiaCandidateFinance(input);

    const directBreakdownCalls = input.db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.va_candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls.length).toBeGreaterThan(0);
    for (const call of directBreakdownCalls) {
      expect(call[1]?.[6]).toBe("https://cfreports.elections.virginia.gov/Report/Xml/123");
    }
  });

  it("preserves non-manual link provenance passed from an existing finance link", async () => {
    const input = baseInput({ linkSource: "cfreports_search" });

    await syncVirginiaCandidateFinance(input);

    expect(input.db.query.mock.calls[0]?.[1]?.[10]).toBe("cfreports_search");
  });

  it("validates required identifiers before writing", async () => {
    const input = baseInput({
      committeeId: " ",
    });

    await expect(syncVirginiaCandidateFinance(input)).rejects.toThrow("Virginia committee id is required");
    expect(input.db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const input = baseInput({
      electionYear: 1999,
    });

    await expect(syncVirginiaCandidateFinance(input)).rejects.toThrow("Invalid Virginia finance election year");
    expect(input.db.query).not.toHaveBeenCalled();
  });
});
