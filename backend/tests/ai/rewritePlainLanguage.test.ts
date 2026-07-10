import { describe, expect, it } from "vitest";

import {
  parseRewritePayload,
  parseVerifyPayload,
  verifyPlainLanguageRewrite,
} from "../../src/ai/rewritePlainLanguage.js";

describe("verifyPlainLanguageRewrite", () => {
  it("fails closed when no provider independent of the rewriter is configured", async () => {
    const result = await verifyPlainLanguageRewrite(
      { kind: "record_description", originalText: "a", rewrittenText: "b" },
      { timeoutMs: 1000 },
      "openai",
      [
        { provider: "openai", model: "gpt-5.4-mini" },
        { provider: "openai", model: "gpt-5.5" },
      ]
    );

    expect(result).toEqual({
      ok: false,
      reason: "no verifier provider independent of rewriter provider openai is configured",
    });
  });
});

describe("parseRewritePayload", () => {
  it("returns the trimmed rewritten text", () => {
    expect(parseRewritePayload({ rewritten_text: "  Plain words.  " })).toBe("Plain words.");
  });

  it("rejects missing, empty, or non-string rewritten_text", () => {
    expect(() => parseRewritePayload({})).toThrow("rewritten_text");
    expect(() => parseRewritePayload({ rewritten_text: "   " })).toThrow("rewritten_text");
    expect(() => parseRewritePayload({ rewritten_text: 7 })).toThrow("rewritten_text");
    expect(() => parseRewritePayload([])).toThrow("Expected JSON object");
    expect(() => parseRewritePayload(null)).toThrow("Expected JSON object");
  });
});

describe("parseVerifyPayload", () => {
  it("parses same_facts without requiring a reason", () => {
    expect(parseVerifyPayload({ verdict: "same_facts" })).toEqual({ verdict: "same_facts", reason: null });
  });

  it("parses mismatch with its reason and normalizes case", () => {
    expect(parseVerifyPayload({ verdict: "MISMATCH", reason: "dropped the negation" })).toEqual({
      verdict: "mismatch",
      reason: "dropped the negation",
    });
  });

  it("rejects a mismatch without a reason", () => {
    expect(() => parseVerifyPayload({ verdict: "mismatch" })).toThrow("requires a reason");
    expect(() => parseVerifyPayload({ verdict: "mismatch", reason: "  " })).toThrow("requires a reason");
  });

  it("rejects unknown verdicts", () => {
    expect(() => parseVerifyPayload({ verdict: "maybe" })).toThrow("Expected verdict");
    expect(() => parseVerifyPayload({})).toThrow("Expected verdict");
  });
});
