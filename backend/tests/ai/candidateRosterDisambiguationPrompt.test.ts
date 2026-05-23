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
  });
});
