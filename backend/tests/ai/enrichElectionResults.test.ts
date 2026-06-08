import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ElectionResultContext } from "../../src/pipeline/electionResults/electionResultContextLoader.js";

const { callResearchProviderMock, verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  callResearchProviderMock: vi.fn(),
  verifyHttpUrlReachabilityMock: vi.fn(),
}));

vi.mock("../../src/ai/researchProviderClient.js", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (text: string) => text,
}));

vi.mock("../../src/ai/urlReachability.js", () => ({
  verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
}));

const ELECTION_ID = "00000000-0000-0000-0000-000000000001";
const CANDIDATE_ELECTION_ID = "10000000-0000-0000-0000-000000000001";
const CANDIDATE_ID = "20000000-0000-0000-0000-000000000001";

function makeContext(): ElectionResultContext {
  return {
    electionId: ELECTION_ID,
    raceType: "office",
    officialBallotTitle: "Governor",
    electionDate: "2026-11-03",
    electionStage: "general",
    isPartisan: true,
    discoveryContestFamily: "non_judicial_office",
    sourceUrls: [],
    district: {
      id: "district-1",
      name: "California",
      districtType: "statewide",
      state: "CA",
    },
    candidates: [
      {
        candidateElectionId: CANDIDATE_ELECTION_ID,
        candidateId: CANDIDATE_ID,
        displayName: "Jane Candidate",
        party: "Democratic",
        isIncumbent: false,
        status: "declared",
        fecIds: [],
        stateFilingIds: [],
      },
    ],
    ballotMeasure: null,
  };
}

describe("enrichElectionResults", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://elections.example.gov/results",
      finalUrl: "https://elections.example.gov/results",
      status: 200,
    });
  });

  it("records prior failed model attempts when a later model succeeds", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: false,
        retryable: false,
        errorCode: "PROVIDER_ERROR",
        reason: "Claude returned an invalid tool response",
        failureDebug: { provider_status: 400 },
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          results: [
            {
              election_id: ELECTION_ID,
              result_status: "unofficial",
              outcome: "won",
              winners: [{ candidate_election_id: CANDIDATE_ELECTION_ID }],
              source_url: "https://elections.example.gov/results",
              source_type: "official",
              notes: "Unofficial result source.",
            },
          ],
        },
        rawText: "{\"results\":[]}",
        debugMeta: { provider_debug: "openai-success" },
      });

    const { enrichElectionResults } = await import("../../src/ai/enrichElectionResults.js");
    const result = await enrichElectionResults(
      {
        passType: "election_night",
        scheduledFor: "2026-11-04T04:10:00.000Z",
        contexts: [makeContext()],
      },
      { timeoutMs: 30_000 },
      [
        { provider: "claude", model: "claude-sonnet-4-6" },
        { provider: "openai", model: "gpt-5.5" },
      ]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }

    expect(result.provider).toBe("openai");
    expect(result.model).toBe("gpt-5.5");
    expect(result.payload.results[0]?.winners[0]).toEqual({
      candidate_election_id: CANDIDATE_ELECTION_ID,
      candidate_id: CANDIDATE_ID,
      candidate_name: "Jane Candidate",
      party: "Democratic",
    });
    expect(result.aiRawDebug?.prior_failed_attempts).toEqual([
      {
        provider: "claude",
        model: "claude-sonnet-4-6",
        reason: "Claude returned an invalid tool response",
        errorCode: "PROVIDER_ERROR",
        retryable: false,
        failureDebug: { provider_status: 400 },
      },
    ]);
  });
});
