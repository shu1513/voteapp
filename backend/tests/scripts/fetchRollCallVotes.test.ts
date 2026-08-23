import { describe, expect, it } from "vitest";

import { parseRollList } from "../../src/scripts/fetchRollCallVotes.js";

describe("parseRollList", () => {
  it("expands ranges, sorts, and de-duplicates", () => {
    expect(parseRollList("190-192,14, 18,14")).toEqual([14, 18, 190, 191, 192]);
    expect(parseRollList("7")).toEqual([7]);
  });

  it("rejects malformed or empty input", () => {
    expect(() => parseRollList("")).toThrow(/names no roll calls/);
    expect(() => parseRollList("a-b")).toThrow(/not a number or range/);
    expect(() => parseRollList("0")).toThrow(/starts below 1/);
    expect(() => parseRollList("20-10")).toThrow(/empty/);
  });
});
