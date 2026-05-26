import { describe, expect, it } from "vitest";

import { buildCandidateRosterPrompt } from "../../src/ai/providers/candidateRosterPrompt.js";

describe("buildCandidateRosterPrompt", () => {
  const baseInput = {
    districtName: "Los Angeles County, California",
    districtType: "county",
    state: "CA",
    electionDate: "2026-06-02",
    officialBallotTitle: "Assessor",
    researchMode: "state_level" as const,
    seedUrls: [],
    reviewFeedbackLines: [],
  };

  it("includes party field for standard contests", () => {
    const prompt = buildCandidateRosterPrompt({
      ...baseInput,
      includeParty: true,
    });
    expect(prompt).toContain('"party": "party label when clearly known (optional)"');
    expect(prompt).not.toContain('"fec_ids":');
    expect(prompt).toContain('"state_filing_ids": ["state filing ID(s) (optional)"]');
  });

  it("omits party field for nonpartisan contests", () => {
    const prompt = buildCandidateRosterPrompt({
      ...baseInput,
      includeParty: false,
    });
    expect(prompt).not.toContain('"party": "party label when clearly known (optional)"');
  });

  it("includes optional senate context fields when provided", () => {
    const prompt = buildCandidateRosterPrompt({
      ...baseInput,
      districtType: "statewide",
      officialBallotTitle: "United States Senator",
      researchMode: "federal_us_senate",
      electionStage: "special",
      senateClass: "class_ii",
      termEndYear: "2029",
      includeParty: true,
    });

    expect(prompt).toContain('- election_stage: "special"');
    expect(prompt).toContain('- senate_class: "class_ii"');
    expect(prompt).toContain('- term_end_year: "2029"');
    expect(prompt).toContain('"fec_ids": ["required FEC candidate ID(s)"]');
    expect(prompt).toContain("fec_ids is required");
    expect(prompt).not.toContain('"state_filing_ids":');
  });

  it("does not include senate context fields for non-senate titles", () => {
    const prompt = buildCandidateRosterPrompt({
      ...baseInput,
      officialBallotTitle: "County Assessor",
      researchMode: "state_level",
      electionStage: "general",
      senateClass: "class_i",
      termEndYear: "2031",
      includeParty: true,
    });

    expect(prompt).not.toContain("- election_stage:");
    expect(prompt).not.toContain("- senate_class:");
    expect(prompt).not.toContain("- term_end_year:");
  });

  it("requires fec_ids in federal us_house mode", () => {
    const prompt = buildCandidateRosterPrompt({
      ...baseInput,
      districtType: "us_house",
      officialBallotTitle: "United States Representative, District 12",
      researchMode: "federal_us_house",
      includeParty: true,
    });

    expect(prompt).toContain('"fec_ids": ["required FEC candidate ID(s)"]');
    expect(prompt).toContain("fec_ids is required");
    expect(prompt).not.toContain('"state_filing_ids":');
  });
});
