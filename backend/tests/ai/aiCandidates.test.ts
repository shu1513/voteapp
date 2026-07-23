import { describe, expect, it } from "vitest";

import {
  DEFAULT_AI_CANDIDATE,
  FRONTIER_AI_CANDIDATES,
} from "../../src/ai/aiCandidates.ts";

describe("frontier AI candidate policy", () => {
  it("uses one model per provider in the requested fallback order", () => {
    expect(FRONTIER_AI_CANDIDATES.map((candidate) => candidate.provider)).toEqual([
      "claude",
      "openai",
      "gemini",
    ]);
    expect(new Set(FRONTIER_AI_CANDIDATES.map((candidate) => candidate.provider)).size).toBe(
      FRONTIER_AI_CANDIDATES.length
    );
    expect(FRONTIER_AI_CANDIDATES.every((candidate) => candidate.model.length > 0)).toBe(true);
  });

  it("uses the first frontier candidate as the global default", () => {
    expect(DEFAULT_AI_CANDIDATE).toBe(FRONTIER_AI_CANDIDATES[0]);
  });
});
