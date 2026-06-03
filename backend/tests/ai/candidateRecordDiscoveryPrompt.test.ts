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
    expect(prompt).toContain("If the action/event date is unknown, use the source publication date.");
    expect(prompt).toContain(
      "If neither action/event date nor publication date is available, omit that record."
    );
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

  it("includes election_stage for non-senate offices when provided", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
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

  it("enforces competence/background objective and hard exclusion for candidacy-only rows", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt(baseInput);

    expect(prompt).toContain(
      "Focus on records that evaluate fitness/competence for this office and the candidate's background relevant to office duties."
    );
    expect(prompt).toContain(
      "Research reliable public records about this exact candidate that show concrete actions or accountability relevant to this office, such as votes, sponsored legislation, official decisions, public policy statements, budgets managed, committee work, finance records, legal/ethics scrutiny, prior government service, professional achievements or failures, and documented positions on key issues."
    );
    expect(prompt).toContain(
      "Include documented criminal convictions, official ethics findings, sanctions, disciplinary actions, court judgments, enforcement actions, or verified public accountability records when they exist. Do not include rumors or unverified accusations."
    );
    expect(prompt).toContain(
      "Hard rule: no pure candidacy announcement/profile rows. Do not return records whose only substance is that the person is running, filed to run, launched a campaign, appears on a ballot, or is listed in a voter guide."
    );
    expect(prompt).not.toContain("Return records only about this exact candidate in this election context");
    expect(prompt).not.toContain("Good records include:");
    expect(prompt).not.toContain("Prefer records that reveal a stance or governing record");
  });
});
