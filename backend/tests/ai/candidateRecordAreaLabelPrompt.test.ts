import { describe, expect, it } from "vitest";

import { buildCandidateRecordAreaLabelPrompt } from "../../src/ai/providers/candidateRecordAreaLabelPrompt.js";

describe("buildCandidateRecordAreaLabelPrompt", () => {
  const baseInput = {
    candidateDisplayName: "Jane Doe",
    districtName: "California",
    districtType: "statewide",
    state: "CA",
    electionDate: "2026-11-03",
    officialBallotTitle: "Governor",
    allowedResearchAreaSlugs: [
      "general",
      "integrity_and_ethics",
      "government_efficiency",
      "public_safety_and_crime_control",
    ],
    records: [
      {
        description: "Supported budget increase for police staffing in city budget vote.",
        sourceUrl: "https://example.org/news/a",
        eventDate: "2026-03-12",
      },
    ],
    reviewFeedbackLines: [],
  };

  it("includes allowed slugs and general stance rule", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt(baseInput);
    expect(prompt).toContain(
      'Allowed research area slugs for this candidate/election context (use only these): ["general","integrity_and_ethics","government_efficiency","public_safety_and_crime_control"]'
    );
    expect(prompt).toContain(
      "Special non-stance areas: use research_area_slug='general' when no specific allowed area applies; use research_area_slug='integrity_and_ethics' for documented criminal convictions"
    );
    expect(prompt).toContain("When research_area_slug is 'general' or 'integrity_and_ethics', omit stance.");
    expect(prompt).toContain("For all other research_area_slug values, stance is required and must be for|against|neutral.");
  });

  it("includes senate context when senate title is used", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt({
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

  it("includes election_stage for non-senate offices when provided", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt({
      ...baseInput,
      officialBallotTitle: "Governor",
      electionStage: "primary",
      senateClass: "class_i",
      termEndYear: "2031",
    });
    expect(prompt).toContain('- election_stage: "primary"');
    expect(prompt).not.toContain("- senate_class:");
    expect(prompt).not.toContain("- term_end_year:");
  });
});
