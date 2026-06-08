import { describe, expect, it, vi } from "vitest";

import { enqueueCandidateProfileDrafts } from "../../src/pipeline/candidates/candidateProfileDraftEmitter.js";

describe("enqueueCandidateProfileDrafts", () => {
  it("emits candidate-profile draft with election/name/index marker", async () => {
    const sendCommand = vi.fn().mockResolvedValueOnce(1);

    const result = await enqueueCandidateProfileDrafts(
      { sendCommand },
      [
        {
          electionId: "e-1",
          runId: "run-1",
          displayName: "Jane Candidate",
          rosterIndex: 2,
          rosterParty: "Democratic",
          rosterIsIncumbent: true,
          disambiguationHint: "Roster candidate.",
          fecIds: [" h123 "],
          stateFilingIdsHint: [" sf-1 "],
          skipPerElectionNameDedupe: true,
          seedUrls: ["https://example.gov/a"],
        },
      ]
    );

    expect(result).toEqual({ emittedCount: 1, skippedCount: 0 });
    expect(sendCommand).toHaveBeenCalledTimes(1);
    const args = sendCommand.mock.calls[0]?.[0] as string[];
    expect(args[0]).toBe("EVAL");
    expect(args[2]).toBe("2");
    expect(args[3]).toBe("staging:candidates:profile:draft");
    expect(args[4]).toContain("staging:candidate_profile_draft_emitted:e-1:jane candidate:2");
    expect(args[6]).toBe("candidate_profile");
    expect(args[8]).toBe("Jane Candidate");
    expect(args[9]).toBe("Democratic");
    expect(args[10]).toBe("true");
    expect(args[12]).toBe("2");
    expect(args[13]).toBe("Roster candidate.");
    expect(args[14]).toBe("true");
    expect(args[15]).toBe(JSON.stringify(["H123"]));
    expect(args[16]).toBe(JSON.stringify(["SF-1"]));
  });

  it("supports explicit dedupe keys for non-roster producers", async () => {
    const sendCommand = vi.fn().mockResolvedValueOnce(1);

    await enqueueCandidateProfileDrafts(
      { sendCommand },
      [
        {
          electionId: "e-1",
          runId: null,
          displayName: "Pat Connolly",
          rosterIndex: 100_000,
          seedUrls: ["https://results.example.gov"],
          dedupeKey: "election_result_winner:e-1:pat connolly",
        },
      ]
    );

    const args = sendCommand.mock.calls[0]?.[0] as string[];
    expect(args[4]).toBe("staging:candidate_profile_draft_emitted:election_result_winner:e-1:pat connolly");
    expect(args[14]).toBe("false");
  });

  it("skips duplicate marker keys in the same batch", async () => {
    const sendCommand = vi.fn().mockResolvedValueOnce(1);

    const result = await enqueueCandidateProfileDrafts(
      { sendCommand },
      [
        {
          electionId: "e-1",
          runId: null,
          displayName: "Jane Candidate",
          rosterIndex: 0,
          seedUrls: [],
        },
        {
          electionId: "e-1",
          runId: null,
          displayName: "Jane Candidate",
          rosterIndex: 0,
          seedUrls: [],
        },
      ]
    );

    expect(result).toEqual({ emittedCount: 1, skippedCount: 1 });
    expect(sendCommand).toHaveBeenCalledTimes(1);
  });
});
