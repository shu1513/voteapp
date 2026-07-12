import { describe, expect, it, vi } from "vitest";

import {
  aggregateNewYorkOutsideGroupFunders,
  getNewYorkOutsideGroupFunderBreakdowns,
} from "../../../src/pipeline/newYorkFinance/newYorkOutsideGroupContributionAggregator.js";
import type { NewYorkCommitteeReceiptRow } from "../../../src/pipeline/newYorkFinance/newYorkSodaClient.js";

function receipt(overrides: Partial<NewYorkCommitteeReceiptRow>): NewYorkCommitteeReceiptRow {
  return {
    entityName: "Uber Technologies Inc.",
    entityFirstName: "",
    entityLastName: "",
    contributorType: null,
    scheduleAbbrev: "B",
    amount: 100,
    ...overrides,
  };
}

describe("aggregateNewYorkOutsideGroupFunders", () => {
  it("aggregates organization donors by normalized name and excludes individuals", () => {
    const result = aggregateNewYorkOutsideGroupFunders({
      receipts: [
        receipt({ amount: 8_872.23 }),
        receipt({ entityName: "UBER  TECHNOLOGIES INC.", amount: 41_200 }),
        receipt({ entityName: "FanDuel Inc.", contributorType: "Corporation", amount: 500 }),
        // Individual (schedule A with a person's name): never company backing.
        receipt({
          entityName: "",
          entityFirstName: "Jane",
          entityLastName: "Donor",
          contributorType: "Individual",
          scheduleAbbrev: "A",
          amount: 10_000,
        }),
        // Explicit individual-ish contributor types are excluded even with an entity name.
        receipt({ entityName: "Jane Donor", contributorType: "Individual", amount: 5_000 }),
        receipt({ entityName: "Family Money", contributorType: "Candidate Family Member", amount: 5_000 }),
      ],
    });

    expect(result.funders).toEqual([
      {
        categoryType: "donor",
        categoryName: "Uber Technologies Inc.",
        amount: 50_072.23,
        contributorCount: 2,
        sourceUrl: "https://data.ny.gov/d/e9ss-239a",
      },
      {
        categoryType: "donor",
        categoryName: "FanDuel Inc.",
        amount: 500,
        contributorCount: 1,
        sourceUrl: "https://data.ny.gov/d/e9ss-239a",
      },
    ]);
    expect(result).toMatchObject({
      receiptRowCount: 6,
      organizationRowCount: 3,
      skippedIndividualRowCount: 3,
    });
  });

  it("caps funders at the limit ordered by amount", () => {
    const result = aggregateNewYorkOutsideGroupFunders({
      receipts: [
        receipt({ entityName: "Org A", amount: 10 }),
        receipt({ entityName: "Org B", amount: 30 }),
        receipt({ entityName: "Org C", amount: 20 }),
      ],
      maxFunders: 2,
    });
    expect(result.funders.map((funder) => funder.categoryName)).toEqual(["Org B", "Org C"]);
    expect(() => aggregateNewYorkOutsideGroupFunders({ receipts: [], maxFunders: 0 })).toThrow(
      "Invalid New York outside group funder limit"
    );
  });
});

describe("getNewYorkOutsideGroupFunderBreakdowns", () => {
  it("fetches cycle-scoped receipts for the group and aggregates them", async () => {
    const getReceipts = vi.fn(async () => [receipt({ amount: 11_686_700.23 })]);
    const result = await getNewYorkOutsideGroupFunderBreakdowns(
      { filerId: "590891", electionYear: 2026, maxFunders: 5 },
      {},
      getReceipts
    );
    expect(getReceipts).toHaveBeenCalledWith({ filerId: "590891", electionYear: 2026 }, {});
    expect(result.funders[0]).toMatchObject({ categoryName: "Uber Technologies Inc.", amount: 11_686_700.23 });
  });
});
