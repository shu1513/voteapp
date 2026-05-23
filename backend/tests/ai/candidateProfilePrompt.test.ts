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

  it("includes disambiguation hint only when provided", () => {
    const withHint = buildCandidateProfilePrompt({
      ...baseInput,
      includeParty: true,
      disambiguationHint: "Democrat on county sample ballot",
    });
    expect(withHint).toContain('- roster_disambiguation_hint: "Democrat on county sample ballot"');
    expect(withHint).toContain("Use roster_disambiguation_hint to target this person only.");
    expect(withHint).toContain(
      "When identity is uncertain, prefer null/omission for identity fields over guessing another person's identifiers."
    );

    const withoutHint = buildCandidateProfilePrompt({
      ...baseInput,
      includeParty: true,
    });
    expect(withoutHint).not.toContain("- roster_disambiguation_hint:");
    expect(withoutHint).not.toContain(
      "When identity is uncertain, prefer null/omission for identity fields over guessing another person's identifiers."
    );
  });
});
