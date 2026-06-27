import { describe, expect, it } from "vitest";

import {
  aggregateFloridaDirectContributions,
  floridaElectionCycleStartYear,
  isFloridaDirectDonorSupportReceipt,
  isFloridaTotalReceipt,
} from "../../../src/pipeline/floridaFinance/floridaDirectContributionAggregator.js";
import type { FloridaContributionRow } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceRows.js";

function contribution(overrides: Partial<FloridaContributionRow> = {}): FloridaContributionRow {
  return {
    recipientName: "Friends of Jane Doe",
    contributionDate: "9/15/2026",
    amount: "250.00",
    transactionType: "CHE",
    contributorName: "Pat Smith",
    address: "1 Main St",
    city: "Tallahassee",
    state: "FL",
    zip: "32301",
    occupation: "Attorney",
    inKindDescription: "",
    electionCode: "20261103-GEN",
    sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
    ...overrides,
  };
}

describe("floridaDirectContributionAggregator", () => {
  it("aggregates direct donor support by occupation and contribution size", () => {
    const sourceUrl = "https://dos.elections.myflorida.com/cgi-bin/contrib.exe";
    const result = aggregateFloridaDirectContributions({
      recipientName: "Friends of Jane Doe",
      electionYear: 2026,
      sourceUrl,
      contributionRows: [
        contribution({ amount: "100.00", occupation: "Attorney" }),
        contribution({
          amount: "$250.00",
          occupation: "Attorney",
          contributorName: "Robin Roe",
          address: "2 Main St",
        }),
        contribution({
          amount: "5,000.00",
          occupation: "Teacher",
          contributorName: "Sam Green",
          address: "3 Main St",
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
        sourceUrl,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 350,
          contributorCount: 2,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$5,000+",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 100,
          contributorCount: 1,
          sourceUrl,
        },
      ],
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
    });
  });

  it("filters matching funds, in-kind rows, invalid amounts, and out-of-cycle rows", () => {
    expect(floridaElectionCycleStartYear(2026)).toBe(2025);

    const result = aggregateFloridaDirectContributions({
      recipientName: "Friends of Jane Doe",
      recipientNames: ["Jane Doe Campaign"],
      electionYear: 2026,
      contributionRows: [
        contribution({ recipientName: "Jane Doe Campaign", contributionDate: "1/1/2025", amount: "200" }),
        contribution({ contributorName: "State of Florida", amount: "100000", occupation: "" }),
        contribution({ transactionType: "INK", amount: "5000", contributorName: "Florida Party", occupation: "" }),
        contribution({ amount: "-100", occupation: "Attorney" }),
        contribution({ amount: "bad", occupation: "Doctor" }),
        contribution({ contributionDate: "12/31/2024", amount: "700", occupation: "Engineer" }),
        contribution({ recipientName: "Other Committee", amount: "900", occupation: "Teacher" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(200);
    expect(result.summary.directContributionTotal).toBe(200);
    expect(result.matchedContributionRowCount).toBe(6);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(5);
    expect(result.directBreakdowns).toEqual([
      expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 200 }),
      expect.objectContaining({ categoryType: "contribution_size", categoryName: "$100-$249", amount: 200 }),
    ]);
  });

  it("counts distinct contributors and skips placeholder occupations without losing size totals", () => {
    const result = aggregateFloridaDirectContributions({
      recipientName: "Friends of Jane Doe",
      electionYear: 2026,
      contributionRows: [
        contribution({ amount: "100", occupation: "Info Requested" }),
        contribution({ amount: "200", occupation: "Attorney" }),
        contribution({ amount: "300", occupation: "Attorney" }),
        contribution({
          amount: "400",
          occupation: "Attorney",
          contributorName: "Robin Roe",
          address: "2 Main St",
        }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(1000);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Attorney", amount: 900, contributorCount: 2 }),
    ]);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "contribution_size", categoryName: "$100-$249", amount: 300 }),
        expect.objectContaining({ categoryType: "contribution_size", categoryName: "$250-$499", amount: 700 }),
      ])
    );
  });

  it("classifies Florida contribution rows as total and direct receipts", () => {
    const row = contribution({ amount: "250" });
    expect(isFloridaTotalReceipt({ row, electionYear: 2026 })).toBe(true);
    expect(isFloridaDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(true);

    for (const excludedRow of [
      contribution({ amount: "0" }),
      contribution({ amount: "-10" }),
      contribution({ amount: "not a number" }),
      contribution({ transactionType: "INK" }),
      contribution({ contributorName: "State of Florida" }),
      contribution({ contributionDate: "20240101" }),
    ]) {
      expect(isFloridaTotalReceipt({ row: excludedRow, electionYear: 2026 })).toBe(false);
      expect(isFloridaDirectDonorSupportReceipt({ row: excludedRow, electionYear: 2026 })).toBe(false);
    }
  });

  it("rejects invalid inputs", () => {
    expect(() =>
      aggregateFloridaDirectContributions({
        recipientName: " ",
        electionYear: 2026,
        contributionRows: [],
      })
    ).toThrow("Florida recipient name is required");
    expect(() =>
      aggregateFloridaDirectContributions({
        recipientName: "Friends of Jane Doe",
        electionYear: 1995,
        contributionRows: [],
      })
    ).toThrow("Invalid Florida direct contribution aggregation election year");
    expect(() =>
      aggregateFloridaDirectContributions({
        recipientName: "Friends of Jane Doe",
        electionYear: 2026,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Florida direct contribution aggregation maxBreakdownsPerCategory");
  });
});
