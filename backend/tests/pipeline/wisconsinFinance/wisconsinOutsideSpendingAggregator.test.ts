import { describe, expect, it, vi } from "vitest";

import {
  aggregateWisconsinOutsideSpending,
  buildWisconsinOutsideGroupBreakdowns,
  toWisconsinOutsideGroups,
  toWisconsinOutsideSummary,
} from "../../../src/pipeline/wisconsinFinance/wisconsinOutsideSpendingAggregator.js";

describe("wisconsinOutsideSpendingAggregator", () => {
  it("converts strict Sunshine IE groups into writer outside groups", () => {
    expect(
      toWisconsinOutsideGroups([
        {
          sponsorId: "12231502",
          sponsorName: "AMERICANS FOR PROSPERITY",
          supportOppose: "support",
          amount: 175000,
          expenditureCount: 2,
        },
      ])
    ).toEqual([
      {
        sponsorId: "12231502",
        sponsorName: "AMERICANS FOR PROSPERITY",
        supportOppose: "support",
        amount: 175000,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
    ]);
  });

  it("summarizes outside support and opposition totals", () => {
    expect(
      toWisconsinOutsideSummary({
        outsideGroups: [
          {
            sponsorId: "1",
            sponsorName: "SUPPORT PAC",
            supportOppose: "support",
            amount: 100.105,
          },
          {
            sponsorId: "2",
            sponsorName: "MORE SUPPORT PAC",
            supportOppose: "support",
            amount: 50.106,
          },
          {
            sponsorId: "3",
            sponsorName: "OPPOSE PAC",
            supportOppose: "oppose",
            amount: 25.201,
          },
        ],
      })
    ).toEqual({
      totalReceipts: null,
      directContributionTotal: null,
      totalDisbursements: null,
      cashOnHand: null,
      outsideSupportTotal: 150.21,
      outsideOpposeTotal: 25.2,
      sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
    });
  });

  it("keeps zero outside totals when no strict IE groups are returned", async () => {
    const getIndependentExpenditureGroups = vi.fn().mockResolvedValue([]);

    await expect(
      aggregateWisconsinOutsideSpending({
        candidateCommitteeName: "Tiffany for Wisconsin",
        electionYear: 2026,
        office: "Governor",
        maxGroups: 5,
        sunshineClient: { getIndependentExpenditureGroups },
      })
    ).resolves.toEqual({
      summary: {
        totalReceipts: null,
        directContributionTotal: null,
        totalDisbursements: null,
        cashOnHand: null,
        outsideSupportTotal: 0,
        outsideOpposeTotal: 0,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
      outsideGroups: [],
      outsideGroupBreakdowns: [],
      classifications: [],
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      outsideGroupCount: 0,
      outsideFunderRowCount: 0,
      skippedOutsideGroupFunderLookupCount: 0,
    });
  });

  it("passes exact candidate, office, district, and limit into the Sunshine client", async () => {
    const getIndependentExpenditureGroups = vi.fn().mockResolvedValue([
      {
        sponsorId: "12231502",
        sponsorName: "AMERICANS FOR PROSPERITY",
        supportOppose: "support",
        amount: 175000,
        expenditureCount: 2,
        sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
      },
      {
        sponsorId: "777",
        sponsorName: "OPPOSE PAC",
        supportOppose: "oppose",
        amount: 10000,
        expenditureCount: 1,
      },
    ]);
    const getOutsideSpenderOrganizationFunders = vi.fn().mockResolvedValue([]);

    await expect(
      aggregateWisconsinOutsideSpending({
        candidateCommitteeName: "  Tiffany   for   Wisconsin  ",
        electionYear: 2026,
        office: "  Governor ",
        district: " District 1 ",
        maxGroups: 5,
        maxFundersPerGroup: 7,
        sunshineClientOptions: { timeoutMs: 1000 },
        sunshineClient: { getIndependentExpenditureGroups, getOutsideSpenderOrganizationFunders },
      })
    ).resolves.toMatchObject({
      outsideSupportTotal: 175000,
      outsideOpposeTotal: 10000,
      outsideGroupCount: 2,
      outsideFunderRowCount: 0,
    });

    expect(getIndependentExpenditureGroups).toHaveBeenCalledWith(
      {
        candidateCommitteeName: "Tiffany for Wisconsin",
        electionYear: 2026,
        office: "Governor",
        district: "District 1",
        limit: 5,
      },
      { timeoutMs: 1000 }
    );
    expect(getOutsideSpenderOrganizationFunders).toHaveBeenCalledWith(
      { entityId: "12231502", electionYear: 2026, limit: 7 },
      { timeoutMs: 1000 }
    );
    expect(getOutsideSpenderOrganizationFunders).toHaveBeenCalledWith(
      { entityId: "777", electionYear: 2026, limit: 7 },
      { timeoutMs: 1000 }
    );
  });

  it("builds outside donor and deterministic industry breakdowns", async () => {
    const getIndependentExpenditureGroups = vi.fn().mockResolvedValue([
      {
        sponsorId: "12231502",
        sponsorName: "AMERICANS FOR PROSPERITY",
        supportOppose: "support",
        amount: 175000,
        expenditureCount: 2,
      },
    ]);
    const getOutsideSpenderOrganizationFunders = vi.fn().mockResolvedValue([
      {
        categoryName: "Wisconsin Conservation Action",
        amount: 50000,
        count: 1,
      },
      {
        categoryName: "Wisconsin Conservation Action",
        amount: 25000,
        count: 1,
      },
    ]);

    await expect(
      aggregateWisconsinOutsideSpending({
        candidateCommitteeName: "Tiffany for Wisconsin",
        electionYear: 2026,
        office: "Governor",
        aiClassificationMinAmount: 25000,
        sunshineClient: { getIndependentExpenditureGroups, getOutsideSpenderOrganizationFunders },
      })
    ).resolves.toMatchObject({
      outsideGroups: [
        {
          sponsorId: "12231502",
          sponsorName: "AMERICANS FOR PROSPERITY",
          supportOppose: "support",
          amount: 175000,
        },
      ],
      outsideGroupBreakdowns: expect.arrayContaining([
        {
          sponsorId: "12231502",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Wisconsin Conservation Action",
          amount: 75000,
          contributorCount: 2,
          sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
        },
        {
          sponsorId: "12231502",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "environmental_group",
          amount: 75000,
          contributorCount: 2,
          sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
        },
      ]),
      outsideFunderRowCount: 2,
      skippedOutsideGroupFunderLookupCount: 0,
    });
  });

  it("builds donor breakdowns and skips failed outside spender funder lookups", async () => {
    const getOutsideSpenderOrganizationFunders = vi
      .fn()
      .mockResolvedValueOnce([{ categoryName: "Strategic Victory Fund", amount: 25000, count: 1 }])
      .mockRejectedValueOnce(new Error("Sunshine unavailable"));

    await expect(
      buildWisconsinOutsideGroupBreakdowns({
        outsideGroups: [
          {
            sponsorId: "1",
            sponsorName: "SUPPORT PAC",
            supportOppose: "support",
            amount: 10000,
          },
          {
            sponsorId: "2",
            sponsorName: "OPPOSE PAC",
            supportOppose: "oppose",
            amount: 5000,
          },
        ],
        electionYear: 2026,
        maxFundersPerGroup: 5,
        sunshineClient: { getOutsideSpenderOrganizationFunders },
      })
    ).resolves.toEqual({
      outsideGroupBreakdowns: [
        {
          sponsorId: "1",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Strategic Victory Fund",
          amount: 25000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.wi.gov/browse-data/transactions",
        },
      ],
      outsideFunderRowCount: 1,
      skippedOutsideGroupFunderLookupCount: 1,
    });
  });

  it("rejects invalid aggregation inputs", async () => {
    await expect(
      aggregateWisconsinOutsideSpending({
        candidateCommitteeName: "Tiffany for Wisconsin",
        electionYear: 1999,
        sunshineClient: { getIndependentExpenditureGroups: vi.fn() },
      })
    ).rejects.toThrow("Invalid Wisconsin outside spending aggregation election year");

    await expect(
      aggregateWisconsinOutsideSpending({
        candidateCommitteeName: "Tiffany for Wisconsin",
        electionYear: 2026,
        maxGroups: 0,
        sunshineClient: { getIndependentExpenditureGroups: vi.fn() },
      })
    ).rejects.toThrow("Invalid Wisconsin outside spending aggregation maxGroups");
  });
});
