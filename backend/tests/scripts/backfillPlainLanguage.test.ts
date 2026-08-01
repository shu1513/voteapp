import { describe, expect, it } from "vitest";

import { parseManualRewritesFile } from "../../src/scripts/backfillPlainLanguage.js";

describe("parseManualRewritesFile", () => {
  it("keys entries by targetId and preserves the file order", () => {
    const { byTargetId, targetIds } = parseManualRewritesFile(
      JSON.stringify([
        { targetId: "a", originalText: "Old A.", rewrittenText: "New A." },
        { targetId: "b", originalText: "Old B.", rewrittenText: "New B." },
      ])
    );
    expect(targetIds).toEqual(["a", "b"]);
    expect(byTargetId.get("a")).toEqual({ originalText: "Old A.", rewrittenText: "New A." });
    expect(byTargetId.get("b")).toEqual({ originalText: "Old B.", rewrittenText: "New B." });
  });

  it("keeps distinct rewrites for rows sharing identical original text", () => {
    // The defect the id-keying exists to prevent: a text-keyed map collapsed
    // two same-text rows onto whichever rewrite came last in the file.
    const shared = "Voted for the annual budget.";
    const { byTargetId } = parseManualRewritesFile(
      JSON.stringify([
        { targetId: "a", originalText: shared, rewrittenText: "Rewrite for A." },
        { targetId: "b", originalText: shared, rewrittenText: "Rewrite for B." },
      ])
    );
    expect(byTargetId.get("a")?.rewrittenText).toBe("Rewrite for A.");
    expect(byTargetId.get("b")?.rewrittenText).toBe("Rewrite for B.");
  });

  it("rejects a targetId that appears twice", () => {
    expect(() =>
      parseManualRewritesFile(
        JSON.stringify([
          { targetId: "a", originalText: "Old.", rewrittenText: "New." },
          { targetId: "a", originalText: "Old.", rewrittenText: "Different new." },
        ])
      )
    ).toThrow(/appears more than once/);
  });

  it("rejects entries with missing fields", () => {
    expect(() => parseManualRewritesFile(JSON.stringify([{ targetId: "a", originalText: "x" }]))).toThrow(
      /rewrittenText/
    );
    expect(() => parseManualRewritesFile(JSON.stringify({ not: "an array" }))).toThrow(/JSON array/);
  });
});
