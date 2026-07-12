import { describe, expect, it, vi } from "vitest";

import {
  aggregateNewYorkDirectContributions,
  collectNewYorkDirectCampaign,
} from "../../../src/pipeline/newYorkFinance/newYorkDirectContributionAggregator.js";
import type { NewYorkCommitteeReceiptRow } from "../../../src/pipeline/newYorkFinance/newYorkSodaClient.js";

function receipt(overrides: Partial<NewYorkCommitteeReceiptRow>): NewYorkCommitteeReceiptRow {
  return {
    entityName: "",
    entityFirstName: "Jane",
    entityLastName: "Donor",
    contributorType: "Individual",
    scheduleAbbrev: "A",
    amount: 50,
    ...overrides,
  };
}

describe("aggregateNewYorkDirectContributions", () => {
  it("builds size, type, and org-donor breakdowns; lumps count toward totals only", () => {
    const result = aggregateNewYorkDirectContributions({
      receipts: [
        receipt({ amount: 50 }),
        receipt({ entityFirstName: "Alex", amount: 120 }),
        receipt({ entityFirstName: "Sam", amount: 5_000 }),
        // Organization donor (Schedule B, type often absent).
        receipt({
          entityName: "Uber Technologies Inc.",
          entityFirstName: "",
          entityLastName: "",
          contributorType: null,
          scheduleAbbrev: "B",
          amount: 41_200,
        }),
        receipt({
          entityName: "UBER  TECHNOLOGIES INC.",
          entityFirstName: "",
          entityLastName: "",
          contributorType: "Corporation",
          scheduleAbbrev: "B",
          amount: 8_800,
        }),
        // Unitemized lump: no identity, no type.
        receipt({ entityFirstName: "", entityLastName: "", contributorType: null, amount: 472_748 }),
      ],
    });

    expect(result.directContributionTotal).toBe(527_918);
    expect(result.receiptRowCount).toBe(6);
    expect(result.lumpRowCount).toBe(1);

    const byType = (categoryType: string) => result.breakdowns.filter((row) => row.categoryType === categoryType);
    expect(byType("contribution_size")).toEqual([
      expect.objectContaining({ categoryName: "$5,000+", amount: 55_000, contributorCount: 3 }),
      expect.objectContaining({ categoryName: "$100-$249", amount: 120, contributorCount: 1 }),
      expect.objectContaining({ categoryName: "$1-$99", amount: 50, contributorCount: 1 }),
    ]);
    expect(byType("contributor_type")).toEqual([
      expect.objectContaining({ categoryName: "Corporation", amount: 8_800 }),
      expect.objectContaining({ categoryName: "Individual", amount: 5_170, contributorCount: 3 }),
    ]);
    // Both Uber spellings merge under one donor.
    expect(byType("donor")).toEqual([
      expect.objectContaining({ categoryName: "Uber Technologies Inc.", amount: 50_000, contributorCount: 2 }),
    ]);
  });

  it("caps each category at the limit and rejects invalid limits", () => {
    const receipts = Array.from({ length: 4 }, (_unused, index) =>
      receipt({ entityFirstName: `Donor${index}`, amount: (index + 1) * 1_000 })
    );
    const result = aggregateNewYorkDirectContributions({ receipts, maxBreakdownsPerCategory: 1 });
    expect(result.breakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(1);
    expect(() => aggregateNewYorkDirectContributions({ receipts: [], maxBreakdownsPerCategory: 0 })).toThrow(
      "Invalid New York direct breakdown limit"
    );
  });
});

describe("collectNewYorkDirectCampaign", () => {
  it("fetches receipts and the expenditure total for the linked committee", async () => {
    const getCommitteeItemizedReceipts = vi.fn(async () => [receipt({ amount: 100 })]);
    const getCommitteeExpenditureTotal = vi.fn(async () => 3_706_000.56);

    const result = await collectNewYorkDirectCampaign(
      { filerId: "16851", electionYear: 2026, cycleYears: 4 },
      {},
      { getCommitteeItemizedReceipts, getCommitteeExpenditureTotal }
    );

    expect(getCommitteeItemizedReceipts).toHaveBeenCalledWith(
      { filerId: "16851", electionYear: 2026, cycleYears: 4 },
      {}
    );
    expect(getCommitteeExpenditureTotal).toHaveBeenCalledWith(
      { filerId: "16851", electionYear: 2026, cycleYears: 4 },
      {}
    );
    expect(result).toMatchObject({
      directContributionTotal: 100,
      totalDisbursements: 3_706_000.56,
      receiptRowCount: 1,
    });
  });
});
