import { describe, expect, it } from "vitest";

import { aggregateNewYorkCityOutsideGroupFunders } from "../../../src/pipeline/newYorkCityFinance/newYorkCityOutsideGroupFunderAggregator.js";

describe("newYorkCityOutsideGroupFunderAggregator", () => {
  it("nets organization refunds and attaches funders only to matching spender groups", () => {
    const result = aggregateNewYorkCityOutsideGroupFunders({
      electionYear: 2025,
      electionCycle: "2025",
      groups: [{
        spenderId: "Z1", spenderName: "Outside Group", supportOppose: "support",
        amount: 200, expenditureCount: 2, sourceUrl: "https://example.test/spending",
      }],
      rows: [
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z1", transactionId: "ICONT:R1", funderName: "Example LLC", funderType: "LLC", amount: 100 },
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z1", transactionId: "IREF:R2", funderName: "example llc", funderType: "LLC", amount: -25 },
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z1", transactionId: "ICONT:R3", funderName: "Private Person", funderType: "IND", amount: 500 },
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z2", transactionId: "ICONT:R4", funderName: "Other LLC", funderType: "LLC", amount: 999 },
      ],
    });
    expect(result).toEqual([expect.objectContaining({
      spenderId: "Z1",
      supportOppose: "support",
      categoryType: "donor",
      categoryName: "Example LLC",
      amount: 75,
      contributorCount: 1,
    })]);
  });
});
