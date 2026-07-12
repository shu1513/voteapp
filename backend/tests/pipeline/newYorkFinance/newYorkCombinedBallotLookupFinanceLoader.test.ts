import { describe, expect, it, vi } from "vitest";

import { mergeNewYorkFinanceSummaryMaps } from "../../../src/pipeline/newYorkFinance/newYorkCombinedBallotLookupFinanceLoader.js";
import type { BallotLookupFinanceSummary } from "../../../src/pipeline/address/ballotLookupFinanceShared.js";

const stateSummary = { source: "NEW_YORK_SODA" } as BallotLookupFinanceSummary;
const citySummary = { source: "NEW_YORK_CITY_CFB" } as BallotLookupFinanceSummary;

describe("mergeNewYorkFinanceSummaryMaps", () => {
  it("merges disjoint state and city summaries", () => {
    const result = mergeNewYorkFinanceSummaryMaps({
      state: new Map([["state-key", stateSummary]]),
      city: new Map([["city-key", citySummary]]),
    });
    expect([...result.keys()]).toEqual(["state-key", "city-key"]);
  });

  it("prefers authoritative CFB data and reports impossible cross-provider collisions", () => {
    const onCollision = vi.fn();
    const result = mergeNewYorkFinanceSummaryMaps({
      state: new Map([["same", stateSummary]]),
      city: new Map([["same", citySummary]]),
      onCollision,
    });
    expect(result.get("same")).toBe(citySummary);
    expect(onCollision).toHaveBeenCalledWith("same");
  });
});
