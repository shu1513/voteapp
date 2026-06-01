import { describe, expect, it } from "vitest";

import { enrichCandidateRecordSourcesRepair } from "../../src/ai/enrichCandidateRecordSourcesRepair.js";

describe("enrichCandidateRecordSourcesRepair", () => {
  it("returns skip-success when badRecords is empty", async () => {
    const result = await enrichCandidateRecordSourcesRepair(
      {
        candidateDisplayName: "Jane Doe",
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Governor",
        blockedUrls: [],
        badRecords: [],
      },
      { timeoutMs: 5000 },
      []
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.repairs).toEqual([]);
  });

  it("returns configuration error when no AI candidates are supplied and badRecords exist", async () => {
    const result = await enrichCandidateRecordSourcesRepair(
      {
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
            title: "Record",
            description: "Desc",
            sourceUrl: "https://bad.example/404",
            sourceName: "Bad",
            eventDate: "2026-01-01",
            failureReason: "404",
          },
        ],
      },
      { timeoutMs: 5000 },
      []
    );

    expect(result.ok).toBe(false);
    if (result.ok) {
      return;
    }
    expect(result.errorCode).toBe("CONFIGURATION_ERROR");
  });
});
