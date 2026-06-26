import { describe, expect, it } from "vitest";

import {
  aggregateArizonaOutsideSpending,
  normalizeArizonaSupportOppose,
} from "../../../src/pipeline/arizonaFinance/arizonaOutsideSpendingAggregator.js";
import type { ArizonaSpotlightIndependentExpenditure } from "../../../src/pipeline/arizonaFinance/arizonaSpotlightClient.js";

function expenditure(
  overrides: Partial<ArizonaSpotlightIndependentExpenditure> = {}
): ArizonaSpotlightIndependentExpenditure {
  return {
    transactionDate: "2023-12-22",
    committeeId: "201000285",
    committeeName: "Toa Pac",
    amount: 5400,
    transactionName: "Elect Katie Hobbs",
    transactionType: "Ind. Expend. (Non-Recall) - cash",
    supportOppose: "Support",
    sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=IndependentExpenditures",
    ...overrides,
  };
}

describe("arizonaOutsideSpendingAggregator", () => {
  it("aggregates support and opposition independent expenditure groups", () => {
    const sourceUrl = "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=IndependentExpenditures";
    const result = aggregateArizonaOutsideSpending({
      electionYear: 2024,
      sourceUrl,
      independentExpenditures: [
        expenditure({ amount: 5400 }),
        expenditure({ amount: 100, committeeName: "Toa   Pac" }),
        expenditure({
          committeeId: "9001",
          committeeName: "Oppose Committee",
          amount: 250,
          supportOppose: "Oppose",
        }),
      ],
    });

    expect(result).toEqual({
      matchedIndependentExpenditureCount: 3,
      includedIndependentExpenditureCount: 3,
      skippedIndependentExpenditureCount: 0,
      summary: {
        supportTotal: 5500,
        opposeTotal: 250,
        sourceUrl,
        groups: [
          {
            committeeId: "201000285",
            committeeName: "Toa Pac",
            supportOppose: "support",
            amount: 5500,
            expenditureCount: 2,
            sourceUrl,
          },
          {
            committeeId: "9001",
            committeeName: "Oppose Committee",
            supportOppose: "oppose",
            amount: 250,
            expenditureCount: 1,
            sourceUrl,
          },
        ],
      },
    });
  });

  it("skips rows without amount, position, committee identity, or cycle date", () => {
    const result = aggregateArizonaOutsideSpending({
      electionYear: 2024,
      independentExpenditures: [
        expenditure({ amount: 0 }),
        expenditure({ amount: Number.NaN }),
        expenditure({ supportOppose: undefined }),
        expenditure({ committeeId: "" }),
        expenditure({ transactionDate: "2021-12-31" }),
        expenditure({ amount: 100 }),
      ],
    });

    expect(result.matchedIndependentExpenditureCount).toBe(6);
    expect(result.includedIndependentExpenditureCount).toBe(1);
    expect(result.skippedIndependentExpenditureCount).toBe(5);
    expect(result.summary?.supportTotal).toBe(100);
  });

  it("returns null summary when no expenditures are included", () => {
    expect(
      aggregateArizonaOutsideSpending({
        electionYear: 2024,
        independentExpenditures: [expenditure({ amount: -1 })],
      }).summary
    ).toBeNull();
  });

  it("normalizes Arizona support/oppose labels", () => {
    expect(normalizeArizonaSupportOppose("Support")).toBe("support");
    expect(normalizeArizonaSupportOppose("Oppose")).toBe("oppose");
    expect(normalizeArizonaSupportOppose(undefined)).toBeNull();
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateArizonaOutsideSpending({
        electionYear: 2001,
        independentExpenditures: [],
      })
    ).toThrow("Invalid Arizona outside spending aggregation election year");
    expect(() =>
      aggregateArizonaOutsideSpending({
        electionYear: 2024,
        independentExpenditures: [],
        maxGroups: 0,
      })
    ).toThrow("Invalid Arizona outside spending aggregation maxGroups");
  });
});
