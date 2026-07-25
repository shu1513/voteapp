import { describe, expect, it } from "vitest";

import { buildCandidateRecordDiscoveryPrompt } from "../../src/ai/providers/candidateRecordDiscoveryPrompt.js";
import { PLAIN_LANGUAGE_STYLE_RULES } from "../../src/ai/providers/promptWritingStyle.js";

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

  it("includes the plain-language style rules", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt(baseInput);

    for (const rule of PLAIN_LANGUAGE_STYLE_RULES) {
      expect(prompt).toContain(rule);
    }
    expect(prompt).toContain("6th-grade reader");
  });

  it("includes since_date for incremental mode prompts", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      sinceDate: "2026-04-16",
    });
    expect(prompt).toContain('- since_date: "2026-04-16"');
    expect(prompt).toContain("event_date >= since_date");
    expect(prompt).toContain(
      "Apply the comprehensiveness and balance rules only within that window; never add older records to balance career history."
    );
  });

  it("includes known_current_office only when provided", () => {
    const withOffice = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      knownCurrentOffice: "Governor of California",
    });
    expect(withOffice).toContain('- known_current_office: "Governor of California"');

    const withoutOffice = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      knownCurrentOffice: null,
    });
    expect(withoutOffice).not.toContain("known_current_office");
  });

  it("scopes executive-power coverage to candidates who held an executive role", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt(baseInput);
    expect(prompt).toContain(
      "executive actions (if they ever held an executive role)"
    );
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
      "If the candidate holds or has EVER held public office, cover each of: major votes and sponsored legislation"
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
      "source_url must not be a social/UGC platform"
    );
    expect(prompt).toContain(
      "For damaging claims, require official/legal sources or reputable news (the importer rejects damaging claims cited to other domains) and do not state allegations as proven facts."
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

  it("states officeholder status as fact when has_held_public_office is true", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      hasHeldPublicOffice: true,
    });

    expect(prompt).toContain(
      "This candidate has held public office (verified fact — do not re-derive it)."
    );
    expect(prompt).toContain("major votes and sponsored legislation");
    expect(prompt).not.toContain("If the candidate holds or has EVER held public office");
    expect(prompt).not.toContain("If they never held public office");
  });

  it("states never-held status as fact when has_held_public_office is false", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      hasHeldPublicOffice: false,
    });

    expect(prompt).toContain("This candidate has NEVER held public office (verified fact");
    expect(prompt).toContain("do not use officeholder framing");
    expect(prompt).toContain(
      "career record; organizations and boards they led or served on and public advocacy; and court, legal, or regulatory records"
    );
    expect(prompt).not.toContain("If the candidate holds or has EVER held public office");
    expect(prompt).not.toContain("major votes and sponsored legislation");
  });

  it("keeps the self-decide rule only when has_held_public_office is unknown", () => {
    for (const hasHeldPublicOffice of [null, undefined]) {
      const prompt = buildCandidateRecordDiscoveryPrompt({
        ...baseInput,
        ...(hasHeldPublicOffice === undefined ? {} : { hasHeldPublicOffice }),
      });
      expect(prompt).toContain("If the candidate holds or has EVER held public office");
      expect(prompt).toContain("If they never held public office");
    }
  });

  it("routes judicial contests to the judicial objective regardless of has_held_public_office", () => {
    const prompt = buildCandidateRecordDiscoveryPrompt({
      ...baseInput,
      officialBallotTitle: "Judge of the Superior Court",
      discoveryContestFamily: "judicial_office",
      hasHeldPublicOffice: false,
    });

    expect(prompt).toContain("this exact judicial candidate");
    expect(prompt).not.toContain("This candidate has NEVER held public office");
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
