import { describe, expect, it } from "vitest";

import { buildCandidateRecordDiscoveryPrompt } from "../../src/ai/providers/candidateRecordDiscoveryPrompt.js";

describe("buildCandidateRecordDiscoveryPrompt", () => {
  const baseInput = {
    candidateDisplayName: "Jane Doe",
    districtName: "California",
    districtType: "statewide",
    state: "CA",
    electionDate: "2026-11-03",
    officialBallotTitle: "Governor",
    seedUrls: [],
    reviewFeedbackLines: [],
  };

  it("includes since_date for incremental mode prompts", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      sinceDate: "2026-04-16",
    });
    expect(prompt).toContain('- since_date: "2026-04-16"');
    expect(prompt).toContain("event_date >= since_date");
  });

  it("omits since_date for full mode prompts", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      sinceDate: null,
    });
    expect(prompt).not.toContain("- since_date:");
    expect(prompt).not.toContain("event_date >= since_date");
  });

  it("includes senate context fields for senate office titles when provided", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      officialBallotTitle: "United States Senator",
      electionStage: "general",
      senateClass: "class_i",
      termEndYear: "2031",
    });

    expect(prompt).toContain('- election_stage: "general"');
    expect(prompt).toContain('- senate_class: "class_i"');
    expect(prompt).toContain('- term_end_year: "2031"');
  });
});
