import { describe, expect, it } from "vitest";

import { aggregateKentuckyOutsideGroupContributions } from "../../../src/pipeline/kentuckyFinance/kentuckyOutsideGroupContributionAggregator.js";
import type { KentuckyKrefContributionRecord } from "../../../src/pipeline/kentuckyFinance/kentuckyKrefClient.js";
import type { KentuckyOutsideSpendingGroup } from "../../../src/pipeline/kentuckyFinance/kentuckyOutsideSpendingAggregator.js";

function contribution(overrides: Partial<KentuckyKrefContributionRecord> = {}): KentuckyKrefContributionRecord {
  return {
    recipientName: "Kentucky Future Project Action Fund",
    toOrganizationName: "Kentucky Future Project Action Fund",
    contributorName: "IBEW Local 369 PAC",
    contributorType: "KY Political Action Committee",
    contributionMode: "DIRECT",
    amount: 25_000,
    receiptDate: "10/25/2023",
    statementType: "30 DAY POST",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<KentuckyOutsideSpendingGroup> = {}): KentuckyOutsideSpendingGroup {
  return {
    committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
    committeeName: "Kentucky Future Project Action Fund",
    supportOppose: "support",
    amount: 100_000,
    sourceUrl: "https://secure.kentucky.gov/kref/publicsearch/IndependentExpenditureSearch",
    ...overrides,
  };
}

describe("kentuckyOutsideGroupContributionAggregator", () => {
  it("backtraces KREF outside spender organization contributions into donor and industry breakdowns by normalized name", () => {
    const sourceUrl = "https://secure.kentucky.gov/kref/publicsearch/ExportContributors";
    const result = aggregateKentuckyOutsideGroupContributions({
      electionYear: 2023,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRecords: [
        contribution({ amount: 20_000 }),
        contribution({ amount: 15_000, contributorName: "IBEW Local 369 PAC" }),
        contribution({
          contributorName: "Sierra Club Kentucky PAC",
          contributorType: "Political Committee",
          amount: 30_000,
        }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Local 369 PAC",
          amount: 35_000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Sierra Club Kentucky PAC",
          amount: 30_000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 35_000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "environmental_group",
          amount: 30_000,
          contributorCount: 1,
          sourceUrl,
        },
      ],
    });
  });

  it("duplicates inferred donor totals across support and oppose groups for the same KREF committee name", () => {
    const result = aggregateKentuckyOutsideGroupContributions({
      electionYear: 2023,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose" })],
      contributionRecords: [contribution({ amount: 50_000 })],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "support",
          categoryType: "donor",
          amount: 50_000,
        }),
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "oppose",
          categoryType: "donor",
          amount: 50_000,
        }),
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "support",
          categoryType: "industry",
          amount: 50_000,
        }),
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "oppose",
          categoryType: "industry",
          amount: 50_000,
        }),
      ])
    );
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
  });

  it("only emits deterministic industry rows above the threshold", () => {
    const result = aggregateKentuckyOutsideGroupContributions({
      electionYear: 2023,
      outsideGroups: [outsideGroup()],
      contributionRecords: [
        contribution({ amount: 24_999.99, contributorName: "IBEW Local 369 PAC" }),
        contribution({ amount: 50_000, contributorName: "Unknown Foundation", contributorType: "Organization" }),
        contribution({ amount: 50_000, contributorName: "Jane Person", contributorType: "Individual" }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({ categoryType: "donor", categoryName: "Unknown Foundation", amount: 50_000 }),
      expect.objectContaining({ categoryType: "donor", categoryName: "IBEW Local 369 PAC", amount: 24_999.99 }),
    ]);
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryType === "industry")).toBe(false);
  });

  it("matches KREF committee names case-insensitively and can derive keys from recipient name", () => {
    const result = aggregateKentuckyOutsideGroupContributions({
      electionYear: 2023,
      outsideGroups: [outsideGroup({ committeeKey: " kentucky future project action fund " })],
      contributionRecords: [
        contribution({ toOrganizationName: undefined, recipientName: "Kentucky Future Project Action Fund", amount: 25_000 }),
        contribution({ toOrganizationName: "Other IEC", recipientName: "Other IEC", amount: 90_000 }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          categoryType: "donor",
          categoryName: "IBEW Local 369 PAC",
          amount: 25_000,
        }),
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 25_000,
        }),
      ])
    );
  });

  it("skips invalid amount, missing contributor, non-cycle, and individual rows", () => {
    const result = aggregateKentuckyOutsideGroupContributions({
      electionYear: 2023,
      outsideGroups: [outsideGroup()],
      contributionRecords: [
        contribution({ amount: 0 }),
        contribution({ amount: -10 }),
        contribution({ amount: Number.NaN }),
        contribution({ contributorName: "" }),
        contribution({ receiptDate: undefined, electionYear: undefined }),
        contribution({ contributorName: "Jane Doe", contributorType: "Individual" }),
        contribution({ amount: 25_000 }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(7);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(6);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "donor", amount: 25_000 }),
        expect.objectContaining({ categoryType: "industry", amount: 25_000 }),
      ])
    );
  });

  it("uses explicit election year when present", () => {
    const result = aggregateKentuckyOutsideGroupContributions({
      electionYear: 2023,
      outsideGroups: [outsideGroup()],
      contributionRecords: [
        contribution({ receiptDate: "01/01/2020", electionYear: 2023, amount: 100 }),
        contribution({ receiptDate: "01/01/2023", electionYear: 2020, amount: 200 }),
        contribution({ receiptDate: undefined, electionYear: 2023, amount: 300 }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([expect.objectContaining({ categoryType: "donor", amount: 400 })])
    );
  });

  it("limits donor and industry breakdowns independently within each outside group support bucket", () => {
    const result = aggregateKentuckyOutsideGroupContributions({
      electionYear: 2023,
      maxBreakdownsPerCategory: 1,
      outsideGroups: [
        outsideGroup({ committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND", supportOppose: "support" }),
        outsideGroup({ committeeKey: "COMMONWEALTH FREEDOM FUND", supportOppose: "support" }),
      ],
      contributionRecords: [
        contribution({
          toOrganizationName: "Kentucky Future Project Action Fund",
          contributorName: "IBEW Local 369 PAC",
          amount: 25_000,
        }),
        contribution({
          toOrganizationName: "Kentucky Future Project Action Fund",
          contributorName: "Sierra Club Kentucky PAC",
          contributorType: "Political Committee",
          amount: 50_000,
        }),
        contribution({
          toOrganizationName: "Commonwealth Freedom Fund",
          contributorName: "IBEW Local 222",
          amount: 75_000,
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toHaveLength(4);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          categoryType: "donor",
          categoryName: "Sierra Club Kentucky PAC",
          amount: 50_000,
        }),
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          categoryType: "industry",
          categoryName: "environmental_group",
          amount: 50_000,
        }),
        expect.objectContaining({
          committeeKey: "COMMONWEALTH FREEDOM FUND",
          categoryType: "donor",
          categoryName: "IBEW Local 222",
          amount: 75_000,
        }),
        expect.objectContaining({
          committeeKey: "COMMONWEALTH FREEDOM FUND",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 75_000,
        }),
      ])
    );
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateKentuckyOutsideGroupContributions({ electionYear: 1999, outsideGroups: [], contributionRecords: [] })
    ).toThrow("Invalid Kentucky outside group contribution election year");
    expect(() =>
      aggregateKentuckyOutsideGroupContributions({
        electionYear: 2023,
        outsideGroups: [],
        contributionRecords: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("maxBreakdownsPerCategory");
    expect(() =>
      aggregateKentuckyOutsideGroupContributions({
        electionYear: 2023,
        outsideGroups: [],
        contributionRecords: [],
        minIndustryAmount: -1,
      })
    ).toThrow("minIndustryAmount");
  });
});
