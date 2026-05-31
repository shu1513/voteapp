import { describe, expect, it } from "vitest";

import { mergeIdentifierLists } from "../../src/pipeline/enrichers/candidateProfileEnricher.js";

describe("mergeIdentifierLists", () => {
  it("preserves existing ids and appends new distinct incoming ids", () => {
    const merged = mergeIdentifierLists(
      ["S123", "S234"],
      ["S234", "S345"]
    );

    expect(merged).toEqual(["S123", "S234", "S345"]);
  });

  it("dedupes case-insensitively and ignores blanks", () => {
    const merged = mergeIdentifierLists(
      ["  abc-1  "],
      ["ABC-1", "   ", "abc-2"]
    );

    expect(merged).toEqual(["abc-1", "abc-2"]);
  });

  it("returns undefined when both inputs are empty/missing", () => {
    expect(mergeIdentifierLists(undefined, undefined)).toBeUndefined();
    expect(mergeIdentifierLists([], ["   "])).toBeUndefined();
  });
});
