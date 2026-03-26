import { describe, expect, it } from "vitest";

import { AI_CANDIDATES, DEFAULT_AI_CANDIDATE } from "../../src/ai/aiCandidates.ts";

describe("AI_CANDIDATES", () => {
  it("contains the two currently approved live-smoke candidates", () => {
    expect(AI_CANDIDATES).toEqual([
      { provider: "openai", model: "gpt-4o-mini" },
      { provider: "claude", model: "claude-haiku-4-5-20251001" },
    ]);
  });

  it("exposes the first entry as default", () => {
    expect(DEFAULT_AI_CANDIDATE).toEqual(AI_CANDIDATES[0]);
  });
});
