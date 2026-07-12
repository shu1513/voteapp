import { describe, expect, it } from "vitest";
import { aggregateLosAngelesOutsideSpending } from "../../../src/pipeline/losAngelesCityFinance/losAngelesOutsideSpendingAggregator.js";

describe("aggregateLosAngelesOutsideSpending", () => {
  it("dedupes reports and groups by spender and side", () => {
    const base = {
      spenderId: "1491022",
      spenderName: "People PAC",
      candidateName: "Karen Bass",
      officeName: "Mayor",
      supportOppose: "support" as const,
      reportUrl: null,
    };
    const result = aggregateLosAngelesOutsideSpending([
      { ...base, expenditureId: "1", amount: 100 },
      { ...base, expenditureId: "1", amount: 100 },
      { ...base, expenditureId: "2", amount: 50 },
    ]);
    expect(result.supportTotal).toBe(150);
    expect(result.groups[0]).toMatchObject({
      amount: 150,
      expenditureCount: 2,
    });
  });
});
