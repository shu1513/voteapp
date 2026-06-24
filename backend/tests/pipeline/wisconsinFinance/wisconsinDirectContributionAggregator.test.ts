import { describe, expect, it, vi } from "vitest";

import {
  aggregateWisconsinDirectContributions,
  toWisconsinDirectBreakdowns,
  toWisconsinDirectSummary,
} from "../../../src/pipeline/wisconsinFinance/wisconsinDirectContributionAggregator.js";

describe("wisconsinDirectContributionAggregator", () => {
  it("converts Sunshine occupation and contribution-size aggregates to direct breakdowns", () => {
    expect(
      toWisconsinDirectBreakdowns({
        occupations: [{ categoryName: "RETIRED", amount: 145679.23, count: 55 }],
        contributionSizes: [{ categoryName: "5000_plus", amount: 50000, count: 10 }],
      })
    ).toEqual([
      {
        categoryType: "occupation",
        categoryName: "RETIRED",
        amount: 145679.23,
        contributorCount: 55,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
      {
        categoryType: "contribution_size",
        categoryName: "5000_plus",
        amount: 50000,
        contributorCount: 10,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
    ]);
  });

  it("derives direct contribution total from contribution-size buckets only", () => {
    expect(
      toWisconsinDirectSummary({
        contributionSizes: [
          { categoryName: "under_100", amount: 123.45, count: 6 },
          { categoryName: "5000_plus", amount: 1000.1, count: 1 },
        ],
      })
    ).toEqual({
      totalReceipts: null,
      directContributionTotal: 1123.55,
      totalDisbursements: null,
      cashOnHand: null,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
    });

    expect(toWisconsinDirectSummary({ contributionSizes: [] })).toMatchObject({
      totalReceipts: null,
      directContributionTotal: null,
    });
  });

  it("fetches direct occupations and contribution sizes with the same entity/year/limit", async () => {
    const getDirectOccupationAggregates = vi.fn(async () => [
      { categoryName: "ATTORNEY", amount: 7500, count: 3 },
    ]);
    const getContributionSizeAggregates = vi.fn(async () => [
      { categoryName: "1000_4999", amount: 7500, count: 3 },
    ]);
    const sunshineClientOptions = { timeoutMs: 1000 };

    await expect(
      aggregateWisconsinDirectContributions({
        entityId: "16621",
        electionYear: 2026,
        maxBreakdownsPerCategory: 5,
        sunshineClientOptions,
        sunshineClient: {
          getDirectOccupationAggregates,
          getContributionSizeAggregates,
        },
      })
    ).resolves.toEqual({
      summary: {
        totalReceipts: null,
        directContributionTotal: 7500,
        totalDisbursements: null,
        cashOnHand: null,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "ATTORNEY",
          amount: 7500,
          contributorCount: 3,
          sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
        },
        {
          categoryType: "contribution_size",
          categoryName: "1000_4999",
          amount: 7500,
          contributorCount: 3,
          sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
        },
      ],
      directOccupationRowCount: 1,
      directContributionSizeRowCount: 1,
    });

    expect(getDirectOccupationAggregates).toHaveBeenCalledWith(
      { entityId: "16621", electionYear: 2026, limit: 5 },
      sunshineClientOptions
    );
    expect(getContributionSizeAggregates).toHaveBeenCalledWith(
      { entityId: "16621", electionYear: 2026, limit: 5 },
      sunshineClientOptions
    );
  });

  it("rejects invalid election years and limits", async () => {
    await expect(
      aggregateWisconsinDirectContributions({ entityId: "16621", electionYear: 1999 })
    ).rejects.toThrow("Invalid Wisconsin direct contribution aggregation election year");
    await expect(
      aggregateWisconsinDirectContributions({ entityId: "16621", electionYear: 2026, maxBreakdownsPerCategory: 0 })
    ).rejects.toThrow("Invalid Wisconsin direct contribution aggregation maxBreakdownsPerCategory");
  });
});
