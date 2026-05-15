import { describe, expect, it } from "vitest";

import { AI_CANDIDATES, DEFAULT_AI_CANDIDATE } from "../../src/ai/aiCandidates.ts";

describe("AI_CANDIDATES", () => {
  it("contains the current ordered model cycle (cheap to expensive)", () => {
    expect(AI_CANDIDATES).toEqual([
      { provider: "openai", model: "gpt-5.4-mini" },
      { provider: "claude", model: "claude-sonnet-4-6" },
      { provider: "gemini", model: "gemini-2.5-pro" },
    ]);
  });

  it("exposes the first entry as default", () => {
    expect(DEFAULT_AI_CANDIDATE).toEqual(AI_CANDIDATES[0]);
  });
});
