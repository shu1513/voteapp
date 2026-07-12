import { describe, expect, it } from "vitest";

import { mergeFinanceSummaryMapsStrict, type BallotLookupFinanceSummary } from "../../../src/pipeline/address/ballotLookupFinanceShared.js";

const summary = { source: "NEW_YORK_SODA" } as BallotLookupFinanceSummary;

describe("mergeFinanceSummaryMapsStrict", () => {
  it("merges disjoint providers", () => {
    expect(mergeFinanceSummaryMapsStrict([
      { source: "state", summaries: new Map([["a", summary]]) },
      { source: "city", summaries: new Map([["b", summary]]) },
    ]).size).toBe(2);
  });

  it("throws instead of silently overwriting", () => {
    expect(() => mergeFinanceSummaryMapsStrict([
      { source: "state", summaries: new Map([["same", summary]]) },
      { source: "city", summaries: new Map([["same", summary]]) },
    ])).toThrow("Duplicate finance summary key from state and city");
  });
});
