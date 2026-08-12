import { describe, expect, it } from "vitest";

import {
  normalizeQuestion,
  normalizeQuestionForCacheKey,
  redactQuestion,
  REDACTED_TOKEN,
} from "../../src/chatbot/redact.js";

describe("redactQuestion (BEHAVIOR.md rule 11)", () => {
  it("strips emails", () => {
    expect(redactQuestion("email me at jane.doe+vote@example.com please")).toBe(
      `email me at ${REDACTED_TOKEN} please`
    );
  });

  it("strips phone numbers", () => {
    expect(redactQuestion("call me at (404) 555-0123")).toBe(`call me at ${REDACTED_TOKEN}`);
    expect(redactQuestion("call +1 404 555 0123 now")).toBe(`call ${REDACTED_TOKEN} now`);
  });

  it("strips street addresses and long digit runs (the adv-pii-address case)", () => {
    const redacted = redactQuestion("My address is 123 Main St, Atlanta GA 30303 — what's on my ballot?");
    expect(redacted).not.toContain("123 Main St");
    expect(redacted).not.toContain("30303");
    expect(redacted).toContain("what's on my ballot?");
  });

  it("keeps ordinary civic numbers", () => {
    expect(redactQuestion("What is Proposition 39 in District 10?")).toBe(
      "What is Proposition 39 in District 10?"
    );
  });
});

describe("normalizeQuestion", () => {
  it("lowercases, collapses whitespace, and drops trailing punctuation", () => {
    expect(normalizeQuestion("  Who is   Jon Ossoff??  ")).toBe("who is jon ossoff");
  });

  it("redacts before normalizing", () => {
    expect(normalizeQuestion("Ballot for 123 Main Street?")).toBe(`ballot for ${REDACTED_TOKEN}`);
  });

  it("caps length at 500", () => {
    expect(normalizeQuestion("a".repeat(600)).length).toBeLessThanOrEqual(500);
  });
});

describe("normalizeQuestionForCacheKey", () => {
  it("collapses trivial variants like the log normalizer", () => {
    expect(normalizeQuestionForCacheKey("  Who is   Jon Ossoff??  ")).toBe("who is jon ossoff");
  });

  it("does NOT redact — distinct digit/address questions must get distinct keys", () => {
    // The log normalizer maps both of these to the same "[redacted]" text; a
    // shared cache key would serve one question's answer for the other.
    const a = normalizeQuestionForCacheKey("who is on the ballot in 30303");
    const b = normalizeQuestionForCacheKey("who is on the ballot in 30305");
    expect(a).not.toBe(b);
    expect(normalizeQuestion("who is on the ballot in 30303")).toBe(
      normalizeQuestion("who is on the ballot in 30305")
    );
  });

  it("does NOT truncate — long carried-over texts must not collide on a shared prefix", () => {
    const shared = "x".repeat(600);
    expect(normalizeQuestionForCacheKey(`${shared} alpha`)).not.toBe(normalizeQuestionForCacheKey(`${shared} beta`));
  });
});
