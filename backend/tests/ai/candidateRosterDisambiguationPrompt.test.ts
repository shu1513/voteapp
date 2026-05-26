import { describe, expect, it } from "vitest";

import { buildCandidateRosterDisambiguationPrompt } from "../../src/ai/providers/candidateRosterDisambiguationPrompt.js";

describe("buildCandidateRosterDisambiguationPrompt", () => {
  it("renders duplicate options and person-level status schema", () => {
    const prompt = buildCandidateRosterDisambiguationPrompt({
      districtName: "Los Angeles County, California",
      districtType: "county",
      state: "CA",
      electionDate: "2026-06-02",
      officialBallotTitle: "Sheriff",
      researchMode: "state_level",
      electionIsPartisan: false,
      duplicateDisplayName: "John Smith",
      options: [
        {
          roster_index: 0,
          party: "Democrat",
          sources: ["https://example.org/a"],
        },
        {
          roster_index: 1,
          party: "Republican",
          sources: ["https://example.org/b"],
        },
      ],
      seedUrls: [],
      reviewFeedbackLines: [],
    });

    expect(prompt).toContain('"status": "clear | ambiguous | same_as_other"');
    expect(prompt).toContain('"same_as_roster_index": 0');
    expect(prompt).not.toContain('"verdict":');
    expect(prompt).toContain("- roster_index: 0");
    expect(prompt).toContain('party_hint: "Democrat"');
    expect(prompt).toContain("- roster_index: 1");
    expect(prompt).toContain('party_hint: "Republican"');
    expect(prompt).toContain("- election_is_partisan: false");
    expect(prompt).not.toContain('"fec_ids":');
  });

  it("includes senate context fields only for senate titles", () => {
    const senatePrompt = buildCandidateRosterDisambiguationPrompt({
      districtName: "California",
      districtType: "statewide",
      state: "CA",
      electionDate: "2026-11-03",
      officialBallotTitle: "United States Senator",
      researchMode: "federal_us_senate",
      electionStage: "special",
      senateClass: "class_i",
      termEndYear: "2031",
      electionIsPartisan: true,
      duplicateDisplayName: "John Smith",
      options: [{ roster_index: 0, sources: ["https://example.org/a"] }],
      seedUrls: [],
      reviewFeedbackLines: [],
    });
    expect(senatePrompt).toContain("- election_stage:");
    expect(senatePrompt).toContain("- senate_class:");
    expect(senatePrompt).toContain("- term_end_year:");
    expect(senatePrompt).toContain('"fec_ids": ["required FEC candidate ID(s)"]');
    expect(senatePrompt).toContain("fec_ids is required");

    const nonSenatePrompt = buildCandidateRosterDisambiguationPrompt({
      districtName: "Los Angeles County, California",
      districtType: "county",
      state: "CA",
      electionDate: "2026-06-02",
      officialBallotTitle: "Sheriff",
      researchMode: "state_level",
      electionStage: "special",
      senateClass: "class_i",
      termEndYear: "2031",
      electionIsPartisan: false,
      duplicateDisplayName: "John Smith",
      options: [{ roster_index: 0, sources: ["https://example.org/a"] }],
      seedUrls: [],
      reviewFeedbackLines: [],
    });
    expect(nonSenatePrompt).not.toContain("- election_stage:");
    expect(nonSenatePrompt).not.toContain("- senate_class:");
    expect(nonSenatePrompt).not.toContain("- term_end_year:");
  });
});
