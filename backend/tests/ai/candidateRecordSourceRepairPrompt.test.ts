import { describe, expect, it } from "vitest";

import { buildCandidateRecordSourceRepairPrompt } from "../../src/ai/providers/candidateRecordSourceRepairPrompt.js";

describe("buildCandidateRecordSourceRepairPrompt", () => {
  it("includes blocked URLs and allows no_replacement", () => {
    const prompt = buildCandidateRecordSourceRepairPrompt({
      candidateDisplayName: "Jane Doe",
      districtName: "California",
      districtType: "statewide",
      state: "CA",
      electionDate: "2026-11-03",
      officialBallotTitle: "Governor",
      blockedUrls: ["https://bad.example/404"],
      badRecords: [
        {
          badIndex: 0,
          description: "Record description",
          sourceUrl: "https://bad.example/404",
          eventDate: "2026-01-01",
          failureReason: "citation fetch returned status 404",
        },
      ],
    });

    expect(prompt).toContain("Blocked URLs (must never be reused):");
    expect(prompt).toContain("https://bad.example/404");
    expect(prompt).toContain('"no_replacement": true');
    expect(prompt).toContain("You may return fewer than all bad_index values");
    expect(prompt).toContain("You may fix description, source_url, and event_date when needed.");
    expect(prompt).not.toContain('"title"');
  });

  it("includes election_stage for non-senate offices when provided", () => {
    const prompt = buildCandidateRecordSourceRepairPrompt({
      candidateDisplayName: "Jane Doe",
      districtName: "California",
      districtType: "statewide",
      state: "CA",
      electionDate: "2026-11-03",
      officialBallotTitle: "Governor",
      electionStage: "primary",
      blockedUrls: [],
      badRecords: [],
    });

    expect(prompt).toContain('- election_stage: "primary"');
    expect(prompt).not.toContain("- senate_class:");
    expect(prompt).not.toContain("- term_end_year:");
  });

  it("omits state for presidential United States prompts", () => {
    const prompt = buildCandidateRecordSourceRepairPrompt({
      candidateDisplayName: "Jane President",
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: "2028-11-07",
      officialBallotTitle: "President of the United States, 2028 general election",
      electionStage: "general",
      blockedUrls: [],
      badRecords: [],
    });

    expect(prompt).toContain('- district_type: "presidential"');
    expect(prompt).not.toContain("- state:");
  });
});
