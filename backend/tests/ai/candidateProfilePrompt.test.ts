import { describe, expect, it } from "vitest";

import { buildCandidateProfilePrompt } from "../../src/ai/providers/candidateProfilePrompt.js";

describe("buildCandidateProfilePrompt", () => {
  const baseInput = {
    candidateDisplayName: "Jane Doe",
    districtName: "Los Angeles County, California",
    districtType: "county",
    state: "CA",
    electionDate: "2026-06-02",
    officialBallotTitle: "Assessor",
    seedUrls: [],
    reviewFeedbackLines: [],
  };

  it("includes party field for standard contests", () => {
    const prompt = buildCandidateProfilePrompt({
      ...baseInput,
      includeParty: true,
      rosterParty: "Democrat",
    });
    expect(prompt).toContain('"party": "party label (optional)"');
    expect(prompt).toContain('- roster_party_hint: "Democrat"');
  });

  it("omits party field and roster party hint for nonpartisan contests", () => {
    const prompt = buildCandidateProfilePrompt({
      ...baseInput,
      includeParty: false,
      rosterParty: "Democrat",
    });
    expect(prompt).not.toContain('"party": "party label (optional)"');
    expect(prompt).not.toContain("- roster_party_hint:");
  });
});
