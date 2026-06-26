import { describe, expect, it } from "vitest";

import { aggregateArizonaOutsideGroupContributions } from "../../../src/pipeline/arizonaFinance/arizonaOutsideGroupContributionAggregator.js";
import type { ArizonaOutsideSpendingGroup } from "../../../src/pipeline/arizonaFinance/arizonaOutsideSpendingAggregator.js";
import type { ArizonaSpotlightIncomeTransaction } from "../../../src/pipeline/arizonaFinance/arizonaSpotlightClient.js";

function income(overrides: Partial<ArizonaSpotlightIncomeTransaction> = {}): ArizonaSpotlightIncomeTransaction {
  return {
    transactionDate: "2023-03-28",
    committeeId: "201000285",
    committeeName: "Toa Pac",
    amount: 100,
    transactionName: "Ogorchock, Jace",
    transactionType: "Contribution from Individuals",
    occupation: "Teacher",
    employer: "Phoenix High School District",
    city: "Tempe",
    state: "AZ",
    zipCode: "85281",
    sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<ArizonaOutsideSpendingGroup> = {}): ArizonaOutsideSpendingGroup {
  return {
    committeeId: "201000285",
    committeeName: "Toa Pac",
    supportOppose: "support",
    amount: 5400,
    expenditureCount: 1,
    sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=IndependentExpenditures",
    ...overrides,
  };
}

describe("arizonaOutsideGroupContributionAggregator", () => {
  it("backtraces supporting outside-group income into donor and industry breakdowns", () => {
    const result = aggregateArizonaOutsideGroupContributions({
      electionYear: 2024,
      outsideGroups: [outsideGroup()],
      incomeTransactions: [
        income({ amount: 100 }),
        income({
          amount: 200,
          transactionName: "Guzman, Kim",
          occupation: "Teacher",
          employer: "Phoenix High School District",
          zipCode: "85282",
        }),
        income({
          amount: 500,
          transactionName: "Energy Transfer LLC",
          occupation: "",
          employer: "",
          zipCode: "85283",
        }),
      ],
    });

    expect(result).toEqual({
      matchedIncomeTransactionCount: 3,
      includedIncomeTransactionCount: 3,
      skippedIncomeTransactionCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeId: "201000285",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 500,
          contributorCount: 1,
          sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income",
        },
        {
          committeeId: "201000285",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Guzman, Kim",
          amount: 200,
          contributorCount: 1,
          sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income",
        },
        {
          committeeId: "201000285",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Ogorchock, Jace",
          amount: 100,
          contributorCount: 1,
          sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income",
        },
        {
          committeeId: "201000285",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 500,
          contributorCount: 1,
          sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income",
        },
        {
          committeeId: "201000285",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "education",
          amount: 300,
          contributorCount: 2,
          sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income",
        },
      ],
    });
  });

  it("classifies organization donor labels before individual employer labels", () => {
    const result = aggregateArizonaOutsideGroupContributions({
      electionYear: 2024,
      outsideGroups: [outsideGroup()],
      incomeTransactions: [
        income({
          amount: 1000,
          transactionName: "IBEW Voluntary PAC",
          occupation: "",
          employer: "",
        }),
        income({
          amount: 2000,
          transactionName: "Smith, Pat",
          occupation: "",
          employer: "Google LLC",
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 1000,
        }),
        expect.objectContaining({
          categoryType: "industry",
          categoryName: "technology",
          amount: 2000,
        }),
      ])
    );
  });

  it("duplicates donor-origin breakdowns across support and oppose groups for the same committee", () => {
    const result = aggregateArizonaOutsideGroupContributions({
      electionYear: 2024,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose" })],
      incomeTransactions: [income({ amount: 1000 })],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ committeeId: "201000285", supportOppose: "support", categoryType: "donor", amount: 1000 }),
        expect.objectContaining({ committeeId: "201000285", supportOppose: "oppose", categoryType: "donor", amount: 1000 }),
        expect.objectContaining({ committeeId: "201000285", supportOppose: "support", categoryType: "industry", amount: 1000 }),
        expect.objectContaining({ committeeId: "201000285", supportOppose: "oppose", categoryType: "industry", amount: 1000 }),
      ])
    );
    expect(result.matchedIncomeTransactionCount).toBe(1);
    expect(result.includedIncomeTransactionCount).toBe(1);
  });

  it("skips nonmatching, invalid, and non-cycle income rows", () => {
    const result = aggregateArizonaOutsideGroupContributions({
      electionYear: 2024,
      outsideGroups: [outsideGroup()],
      incomeTransactions: [
        income({ amount: 0 }),
        income({ amount: Number.NaN }),
        income({ transactionDate: "2021-12-31" }),
        income({ committeeId: "OTHER", amount: 1000 }),
        income({ amount: 500 }),
      ],
    });

    expect(result.matchedIncomeTransactionCount).toBe(4);
    expect(result.includedIncomeTransactionCount).toBe(1);
    expect(result.skippedIncomeTransactionCount).toBe(3);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([expect.objectContaining({ categoryType: "donor", amount: 500 })])
    );
  });

  it("honors minimum industry amount without dropping donor breakdowns", () => {
    const result = aggregateArizonaOutsideGroupContributions({
      electionYear: 2024,
      outsideGroups: [outsideGroup()],
      minIndustryAmount: 250,
      incomeTransactions: [
        income({ amount: 249.99, employer: "Phoenix High School District", occupation: "Teacher" }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({ categoryType: "donor", amount: 249.99 }),
    ]);
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryType === "industry")).toBe(false);
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateArizonaOutsideGroupContributions({
        electionYear: 2001,
        outsideGroups: [],
        incomeTransactions: [],
      })
    ).toThrow("Invalid Arizona outside group contribution election year");
    expect(() =>
      aggregateArizonaOutsideGroupContributions({
        electionYear: 2024,
        outsideGroups: [],
        incomeTransactions: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Arizona outside group contribution maxBreakdownsPerCategory");
    expect(() =>
      aggregateArizonaOutsideGroupContributions({
        electionYear: 2024,
        outsideGroups: [],
        incomeTransactions: [],
        minIndustryAmount: -1,
      })
    ).toThrow("Invalid Arizona outside group contribution minIndustryAmount");
  });
});
