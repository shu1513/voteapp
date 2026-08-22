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
    expect(prompt).toContain("For all other research_area_slug values, stance is required and must be for|against.");
    expect(prompt).toContain('"stance": "for | against"');
  });

  it("defines stance against the area goal and asks for every affected area, fiscal counter-tag included", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt(baseInput);
    expect(prompt).toContain(
      "stance 'for' means the record's action directly and materially advances that area's goal; 'against' means it directly and materially cuts against that goal."
    );
    expect(prompt).toContain("Tag EVERY allowed area the action directly affects, each with its own stance.");
    expect(prompt).toContain(
      "a vote raising school funding is public_education_quality 'for' AND government_spending_reduction 'against'"
    );
    expect(prompt).toContain("also tag government_spending_reduction if it is allowed");
    expect(prompt).toContain("Do not tag indirect, speculative, or second-order effects");
    expect(prompt).toContain(
      "An objection about cost or process is a stance on spending, not on the service itself"
    );
    expect(prompt).toContain("For government_spending_reduction specifically, also skip trivial or routine sums");
    expect(prompt).toContain("Prefer fewer, confident labels");
    expect(prompt).not.toContain("You may assign multiple area labels");
  });

  it("lists research area goals when provided and skips blank descriptions", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt({
      ...baseInput,
      allowedResearchAreaGoals: [
        { slug: "government_efficiency", description: "Improve service delivery, reduce waste." },
        { slug: "general", description: null },
        { slug: "public_safety_and_crime_control", description: "  " },
      ],
    });
    expect(prompt).toContain("Research area goals (stance is measured against these):");
    expect(prompt).toContain("- government_efficiency: Improve service delivery, reduce waste.");
    expect(prompt).not.toContain("- general:");
    expect(prompt).not.toContain("- public_safety_and_crime_control:");
  });

  it("omits the goals section when no goals are provided", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt(baseInput);
    expect(prompt).not.toContain("Research area goals");
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

  it("omits state for presidential United States prompts", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt({
      ...baseInput,
      candidateDisplayName: "Jane President",
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: "2028-11-07",
      officialBallotTitle: "President of the United States, 2028 general election",
      electionStage: "general",
    });

    expect(prompt).toContain('- district_type: "presidential"');
    expect(prompt).not.toContain("- state:");
  });

  // These four rules exist because the labeler produced 825 source-verified stance
  // overclaims (repair campaign, 2026-08). Each line targets one measured defect
  // family; removing one silently reopens that family on every new record.
  it("states the no-position gate so rosters and procedural rows are not stanced", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt(baseInput);
    expect(prompt).toContain("FIRST decide whether each record states a position at all");
    expect(prompt).toContain("state NO position: label such a record 'general'");
    expect(prompt).toContain("A measure's procedural fate never erases the candidate's own position on it");
  });

  it("states that stance follows the position's direction, not the vote's surface verb", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt(baseInput);
    expect(prompt).toContain("Stance follows the DIRECTION of the position, never the surface verb");
    expect(prompt).toContain("a no vote on a gerrymandered map is FOR election integrity");
  });

  it("applies materiality to every area, not only government_spending_reduction", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt(baseInput);
    expect(prompt).toContain("Materiality applies to EVERY area, not only spending");
    expect(prompt).toContain("A topic word appearing in the description is not a position on the area");
  });

  it("keeps the spending-specific trivial-sum carve-out alongside the general rule", () => {
    const prompt = buildCandidateRecordAreaLabelPrompt(baseInput);
    expect(prompt).toContain("For government_spending_reduction specifically");
    expect(prompt).toContain("trivial or routine sums");
  });
});
