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
    researchMode: "state_level" as const,
    seedUrls: [],
    reviewFeedbackLines: [],
  };

  it("does not include party field even for standard contests", () => {
    const prompt = buildCandidateProfilePrompt({
      ...baseInput,
      rosterParty: "Democrat",
      rosterStateFilingIds: ["CA-123"],
    });
    expect(prompt).not.toContain('"party": "party label (optional)"');
    expect(prompt).not.toContain('- party: "Democrat"');
    expect(prompt).not.toContain('"fec_ids":');
    expect(prompt).toContain('- candidate_state_filing_ids: ["CA-123"]');
  });

  it("also omits party field for nonpartisan contests", () => {
    const prompt = buildCandidateProfilePrompt({
      ...baseInput,
      rosterParty: "Democrat",
    });
    expect(prompt).not.toContain('"party": "party label (optional)"');
    expect(prompt).not.toContain("- party:");
  });

  it("includes disambiguation hint only when provided", () => {
    const withHint = buildCandidateProfilePrompt({
      ...baseInput,
      disambiguationHint: "Democrat on county sample ballot",
    });
    expect(withHint).toContain('- roster_disambiguation_hint: "Democrat on county sample ballot"');
    expect(withHint).toContain("Use roster_disambiguation_hint to target this person only.");
    expect(withHint).toContain(
      "When identity is uncertain, prefer null/omission for identity fields over guessing another person's identifiers."
    );

    const withoutHint = buildCandidateProfilePrompt({
      ...baseInput,
    });
    expect(withoutHint).not.toContain("- roster_disambiguation_hint:");
    expect(withoutHint).not.toContain(
      "When identity is uncertain, prefer null/omission for identity fields over guessing another person's identifiers."
    );
  });

  it("includes optional senate context fields when provided", () => {
    const prompt = buildCandidateProfilePrompt({
      ...baseInput,
      districtType: "statewide",
      officialBallotTitle: "United States Senator",
      researchMode: "federal_us_senate",
      electionStage: "general",
      senateClass: "class_i",
      termEndYear: "2031",
    });

    expect(prompt).toContain('- election_stage: "general"');
    expect(prompt).toContain('- senate_class: "class_i"');
    expect(prompt).toContain('- term_end_year: "2031"');
    expect(prompt).not.toContain('"fec_ids":');
    expect(prompt).not.toContain('"date_of_birth":');
    expect(prompt).not.toContain("Do not include fec_ids in output; backend provides candidate_fec_ids from roster context.");
    expect(prompt).toContain("do not include date_of_birth; backend stores it as null.");
    expect(prompt).not.toContain("candidate_state_filing_ids");
  });

  it("does not include senate context fields for non-senate titles", () => {
    const prompt = buildCandidateProfilePrompt({
      ...baseInput,
      officialBallotTitle: "County Assessor",
      researchMode: "state_level",
      electionStage: "general",
      senateClass: "class_i",
      termEndYear: "2031",
    });

    expect(prompt).not.toContain("- election_stage:");
    expect(prompt).not.toContain("- senate_class:");
    expect(prompt).not.toContain("- term_end_year:");
  });

  it("uses candidate_fec_ids context in federal us_house mode", () => {
    const prompt = buildCandidateProfilePrompt({
      ...baseInput,
      districtType: "us_house",
      officialBallotTitle: "United States Representative, District 12",
      researchMode: "federal_us_house",
      rosterFecIds: ["H0CA12000"],
    });

    expect(prompt).not.toContain('"fec_ids":');
    expect(prompt).not.toContain('"date_of_birth":');
    expect(prompt).not.toContain("Do not include fec_ids in output; backend provides candidate_fec_ids from roster context.");
    expect(prompt).toContain('- candidate_fec_ids: ["H0CA12000"]');
    expect(prompt).not.toContain("candidate_state_filing_ids");
  });
});
