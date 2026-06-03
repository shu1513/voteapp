import { describe, expect, it } from "vitest";

import { enrichCandidateRecordAreas } from "../../src/ai/enrichCandidateRecordAreas.js";

describe("enrichCandidateRecordAreas", () => {
  it("returns configuration error when no AI candidates are supplied", async () => {
    const result = await enrichCandidateRecordAreas(
      {
        candidateDisplayName: "Jane Doe",
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Governor",
        allowedResearchAreaSlugs: ["general", "government_efficiency"],
        records: [
          {
            description: "Record description",
            sourceUrl: "https://example.org/a",
            eventDate: "2026-05-01",
          },
        ],
      },
      {
        timeoutMs: 5_000,
      },
      []
    );

    expect(result.ok).toBe(false);
    if (result.ok) {
      return;
    }
    expect(result.errorCode).toBe("CONFIGURATION_ERROR");
  });
});
