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
      "If the candidate holds or has EVER held public office, cover each of: major votes they cast and legislation they sponsored"
    );
    expect(prompt).toContain(
      "records may be an empty array if no reliable actual action/service/accountability records are found."
    );
    expect(prompt).toContain(
      "Official ballot, Secretary of State, election-office, or qualified-candidate listings are roster evidence, not candidate record evidence."
    );
    expect(prompt).toContain(
      "Do not include filing-to-run, candidacy announcements, ballot qualification, ballot listing, campaign launch, or campaign promise rows as records."
    );
    expect(prompt).toContain(
      'If the only reliable sources prove the person is running but do not show an actual action, public service, leadership role, vote, official decision, litigation/enforcement record, endorsement, or other accountability record, return {"records": []}.'
    );
    expect(prompt).toContain(
      "For damaging claims, require official/legal sources or reputable news and do not state allegations as proven facts."
    );
    expect(prompt).toContain("There is no target number of records.");
    expect(prompt).toContain("Include both favorable and unfavorable records when they exist");
    expect(prompt).not.toContain("Do not include rumors or unverified accusations.");
    expect(prompt).not.toContain("Starting reference URLs");
    expect(prompt).not.toContain("Return records only about this exact candidate in this election context");
    expect(prompt).not.toContain("Good records include:");
    expect(prompt).not.toContain("Prefer records that reveal a stance or governing record");
  });

  it("omits state for presidential United States prompts", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      candidateDisplayName: "Jane President",
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: "2028-11-07",
      officialBallotTitle: "President of the United States, 2028 Democratic primary",
      electionStage: "primary",
    });

    expect(prompt).toContain('- district_type: "presidential"');
    expect(prompt).not.toContain("- state:");
    expect(prompt).toContain(
      '- official_ballot_title: "President of the United States, 2028 Democratic primary"'
    );
  });

  it("uses judicial record objective only when discovery family is judicial_office", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      officialBallotTitle: "Judge of the Superior Court",
      discoveryContestFamily: "judicial_office",
    });

    expect(prompt).toContain('- discovery_contest_family: "judicial_office"');
    expect(prompt).toContain(
      "notable cases or rulings they handled (as judge, or as prosecutor/defense/counsel before taking the bench)"
    );
    expect(prompt).toContain(
      "any discipline, ethics complaints, reversals, or conduct-commission proceedings; and endorsements they made or received"
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
      "If the candidate holds or has EVER held public office"
    );
  });
});
