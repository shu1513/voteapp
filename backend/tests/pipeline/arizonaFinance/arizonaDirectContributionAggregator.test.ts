import { describe, expect, it } from "vitest";

import { aggregateArizonaDirectContributions } from "../../../src/pipeline/arizonaFinance/arizonaDirectContributionAggregator.js";
import type { ArizonaSpotlightIncomeTransaction } from "../../../src/pipeline/arizonaFinance/arizonaSpotlightClient.js";

function income(overrides: Partial<ArizonaSpotlightIncomeTransaction> = {}): ArizonaSpotlightIncomeTransaction {
  return {
    transactionDate: "2023-01-01",
    committeeId: "201800057",
    committeeName: "Elect Katie Hobbs",
    amount: 250,
    transactionName: "Doe, Jane",
    transactionType: "Contribution from Individuals",
    occupation: "Teacher",
    employer: "Phoenix Union High School District",
    city: "Phoenix",
    state: "AZ",
    zipCode: "85001",
    sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income",
    ...overrides,
  };
}

describe("arizonaDirectContributionAggregator", () => {
  it("aggregates candidate income rows by occupation and contribution size", () => {
    const sourceUrl = "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income";
    const result = aggregateArizonaDirectContributions({
      committeeId: "201800057",
      electionYear: 2024,
      sourceUrl,
      incomeTransactions: [
        income({ amount: 100, occupation: "Teacher" }),
        income({
          amount: 250,
          occupation: "Teacher",
          transactionName: "Roe, John",
          zipCode: "85002",
        }),
        income({
          amount: 5000,
          occupation: "Attorney",
          transactionName: "Smith, Pat",
          zipCode: "85003",
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
          categoryName: "Attorney",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "occupation",
          categoryName: "Teacher",
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
      matchedIncomeTransactionCount: 3,
      includedIncomeTransactionCount: 3,
      skippedIncomeTransactionCount: 0,
    });
  });

  it("counts distinct contributors instead of rows for occupation totals", () => {
    const result = aggregateArizonaDirectContributions({
      committeeId: "201800057",
      electionYear: 2024,
      incomeTransactions: [
        income({ amount: 100, occupation: "Teacher" }),
        income({ amount: 200, occupation: "Teacher" }),
        income({
          amount: 300,
          occupation: "Teacher",
          transactionName: "Roe, John",
          zipCode: "85002",
        }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Teacher", amount: 600, contributorCount: 2 }),
    ]);
  });

  it("does not split the same contributor when employer details change", () => {
    const result = aggregateArizonaDirectContributions({
      committeeId: "201800057",
      electionYear: 2024,
      incomeTransactions: [
        income({ amount: 100, occupation: "Teacher", employer: "Phoenix High School District" }),
        income({ amount: 200, occupation: "Teacher", employer: "Retired" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Teacher", amount: 300, contributorCount: 1 }),
    ]);
  });

  it("keeps broad receipts separate from contribution-only direct support", () => {
    const result = aggregateArizonaDirectContributions({
      committeeId: "201800057",
      electionYear: 2024,
      incomeTransactions: [
        income({ amount: 100, transactionType: "Contribution from Individuals", occupation: "Teacher" }),
        income({ amount: 500, transactionType: "Loan Proceeds", occupation: "Attorney" }),
        income({ amount: 25, transactionType: "Interest Income", occupation: "Banker" }),
      ],
    });

    expect(result.summary).toMatchObject({
      totalReceipts: 625,
      directContributionTotal: 100,
    });
    expect(result.includedIncomeTransactionCount).toBe(1);
    expect(result.skippedIncomeTransactionCount).toBe(2);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([expect.objectContaining({ categoryType: "occupation", categoryName: "Teacher", amount: 100 })])
    );
    expect(result.directBreakdowns.some((row) => row.categoryName === "Attorney")).toBe(false);
  });

  it("matches committee IDs case-insensitively and skips invalid or non-cycle rows", () => {
    const result = aggregateArizonaDirectContributions({
      committeeId: " abc123 ",
      committeeIds: ["201800057"],
      electionYear: 2024,
      incomeTransactions: [
        income({ committeeId: "ABC123", amount: 0 }),
        income({ committeeId: "201800057", amount: 100, occupation: "Teacher" }),
        income({ committeeId: "201800057", amount: 200, transactionDate: "2021-12-31", occupation: "Attorney" }),
        income({ committeeId: "OTHER", amount: 300, occupation: "Doctor" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(100);
    expect(result.matchedIncomeTransactionCount).toBe(3);
    expect(result.includedIncomeTransactionCount).toBe(1);
    expect(result.skippedIncomeTransactionCount).toBe(2);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([expect.objectContaining({ categoryType: "occupation", categoryName: "Teacher" })])
    );
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateArizonaDirectContributions({
        committeeId: "",
        electionYear: 2024,
        incomeTransactions: [],
      })
    ).toThrow("Arizona committee id is required");
    expect(() =>
      aggregateArizonaDirectContributions({
        committeeId: "1",
        electionYear: 2001,
        incomeTransactions: [],
      })
    ).toThrow("Invalid Arizona direct contribution aggregation election year");
    expect(() =>
      aggregateArizonaDirectContributions({
        committeeId: "1",
        electionYear: 2024,
        incomeTransactions: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Arizona direct contribution aggregation maxBreakdownsPerCategory");
  });
});
