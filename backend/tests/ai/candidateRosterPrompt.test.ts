import { describe, expect, it } from "vitest";

import { buildCandidateRosterPrompt } from "../../src/ai/providers/candidateRosterPrompt.js";

describe("buildCandidateRosterPrompt", () => {
  const baseInput = {
    districtName: "Los Angeles County, California",
    districtType: "county",
    state: "CA",
    electionDate: "2026-06-02",
    officialBallotTitle: "Assessor",
    seedUrls: [],
    reviewFeedbackLines: [],
  };

  it("includes party field for standard contests", () => {
    const prompt = buildCandidateRosterPrompt({
      ...baseInput,
      includeParty: true,
    });
    expect(prompt).toContain('"party": "party label when clearly known (optional)"');
  });

  it("omits party field for nonpartisan contests", () => {
    const prompt = buildCandidateRosterPrompt({
      ...baseInput,
      includeParty: false,
    });
    expect(prompt).not.toContain('"party": "party label when clearly known (optional)"');
  });
});
