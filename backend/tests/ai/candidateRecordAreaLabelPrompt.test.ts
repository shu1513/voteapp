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
    allowedResearchAreaSlugs: ["general", "government_efficiency", "public_safety_and_crime_control"],
    records: [
      {
        title: "Backed police staffing expansion plan",
        description: "Supported budget increase for police staffing in city budget vote.",
        sourceUrl: "https://example.org/news/a",
        sourceName: "Example News",
        eventDate: "2026-03-12",
      },
    ],
    reviewFeedbackLines: [],
  };

  it("includes allowed slugs and general stance rule", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt(baseInput);
    expect(prompt).toContain(
      'Allowed research area slugs for this office (use only these): ["general","government_efficiency","public_safety_and_crime_control"]'
    );
    expect(prompt).toContain("When research_area_slug='general', omit stance.");
    expect(prompt).toContain("When research_area_slug!='general', stance is required and must be for|against|neutral.");
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
});
