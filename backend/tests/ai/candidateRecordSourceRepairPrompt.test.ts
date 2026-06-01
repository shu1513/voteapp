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
          title: "Record title",
          description: "Record description",
          sourceUrl: "https://bad.example/404",
          sourceName: "Bad Source",
          eventDate: "2026-01-01",
          failureReason: "citation fetch returned status 404",
        },
      ],
    });

    expect(prompt).toContain("Blocked URLs (must never be reused):");
    expect(prompt).toContain("https://bad.example/404");
    expect(prompt).toContain('"no_replacement": true');
    expect(prompt).toContain("You may return fewer than all bad_index values");
    expect(prompt).toContain("title, description, and event_date are immutable");
  });
});
