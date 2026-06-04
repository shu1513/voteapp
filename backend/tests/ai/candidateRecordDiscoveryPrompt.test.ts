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
    expect(prompt).toContain(
      "event_date must be YYYY-MM-DD; use the action/event date when known, otherwise use the source publication date."
    );
    expect(prompt).toContain(
      "If neither action/event date nor publication date is available, omit that record."
    );
    expect(prompt).not.toContain('"title"');
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
      "Research reliable public records about this exact candidate that show concrete actions or accountability such as votes, sponsored legislation, official decisions, public policy statements, budgets managed, committee work, finance records, legal/ethics scrutiny/documented criminal convictions, prior government service, professional achievements or failures, and documented positions on key issues."
    );
    expect(prompt).toContain(
      "Do not include pure candidacy announcements, such as records whose only substance is that the person is running, filed to run, launched a campaign, appears on a ballot, or is listed in a voter guide."
    );
    expect(prompt).not.toContain("Do not include rumors or unverified accusations.");
    expect(prompt).not.toContain("Starting reference URLs");
    expect(prompt).not.toContain("Return records only about this exact candidate in this election context");
    expect(prompt).not.toContain("Good records include:");
    expect(prompt).not.toContain("Prefer records that reveal a stance or governing record");
  });

  it("uses judicial record objective only when discovery family is judicial_office", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      officialBallotTitle: "Judge of the Superior Court",
      discoveryContestFamily: "judicial_office",
    });

    expect(prompt).toContain('- discovery_contest_family: "judicial_office"');
    expect(prompt).toContain(
      "Research reliable public records about this exact judicial candidate that show legal competence, ethics, and documented legal record"
    );
    expect(prompt).toContain(
      "Describe what the candidate actually did in the case and its effects/impacts."
    );
    expect(prompt).not.toContain("votes, sponsored legislation, official decisions");
  });

  it("does not use judicial record objective from title alone", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      officialBallotTitle: "Judge of the Superior Court",
    });

    expect(prompt).not.toContain("- discovery_contest_family:");
    expect(prompt).not.toContain("this exact judicial candidate");
    expect(prompt).toContain(
      "Research reliable public records about this exact candidate that show concrete actions or accountability"
    );
  });
});
