import { describe, expect, it } from "vitest";

import { parseElectionResultPayload } from "../../src/contracts/electionResultPayloadContract.js";
import type { ElectionResultContext } from "../../src/pipeline/electionResults/electionResultContextLoader.js";

const CANDIDATE_ELECTION_ID = "10000000-0000-0000-0000-000000000001";
const CANDIDATE_ID = "20000000-0000-0000-0000-000000000001";
const ELECTION_ID = "00000000-0000-0000-0000-000000000001";

function makeContext(overrides: Partial<ElectionResultContext> = {}): ElectionResultContext {
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
    ...overrides,
  };
}

function validOfficePayload(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    results: [
      {
        election_id: ELECTION_ID,
        result_status: "unofficial",
        outcome: "won",
        winners: [
          {
            candidate_election_id: CANDIDATE_ELECTION_ID,
          },
        ],
        source_url: "https://elections.example.gov/results",
        source_type: "official",
        ...overrides,
      },
    ],
  };
}

describe("parseElectionResultPayload", () => {
  it("parses office result payloads and derives matched status", () => {
    const parsed = parseElectionResultPayload(validOfficePayload(), {
      passType: "election_night",
      contexts: [makeContext()],
    });

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.results[0]?.match_status).toBe("matched");
      expect(parsed.payload.results[0]?.source_url).toBe("https://elections.example.gov/results");
      expect(parsed.payload.results[0]?.notes).toBe("");
      expect(parsed.payload.results[0]?.winners[0]).toEqual({
        candidate_election_id: CANDIDATE_ELECTION_ID,
        candidate_id: CANDIDATE_ID,
        candidate_name: "Jane Candidate",
        party: "Democratic",
      });
    }
  });

  it("fills roster ids when winner name and party match the election roster", () => {
    const parsed = parseElectionResultPayload(
      validOfficePayload({
        winners: [
          {
            candidate_name: "Jane Candidate",
            party: "Democrat",
          },
        ],
      }),
      {
        passType: "election_night",
        contexts: [makeContext({ candidates: [{ ...makeContext().candidates[0]!, displayName: "Jane Candidate" }] })],
      }
    );

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.results[0]?.match_status).toBe("matched");
      expect(parsed.payload.results[0]?.winners[0]?.candidate_election_id).toBe(CANDIDATE_ELECTION_ID);
      expect(parsed.payload.results[0]?.winners[0]?.candidate_id).toBe(CANDIDATE_ID);
    }
  });

  it("rejects results for election ids outside the provided context", () => {
    const parsed = parseElectionResultPayload(
      validOfficePayload({ election_id: "00000000-0000-0000-0000-000000000999" }),
      {
        passType: "election_night",
        contexts: [makeContext()],
      }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("outside provided context");
  });

  it("rejects AP/news sources unless the result is projected", () => {
    const parsed = parseElectionResultPayload(validOfficePayload({ source_type: "ap" }), {
      passType: "election_night",
      contexts: [makeContext()],
    });

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("source_type ap is incompatible");
  });

  it("allows projected results from AP/news", () => {
    const parsed = parseElectionResultPayload(
      validOfficePayload({
        result_status: "projected",
        outcome: "won",
        source_type: "ap",
      }),
      {
        passType: "election_night",
        contexts: [makeContext()],
      }
    );

    expect(parsed.ok).toBe(true);
  });

  it("rejects certified status on election-night pass", () => {
    const parsed = parseElectionResultPayload(validOfficePayload({ result_status: "certified" }), {
      passType: "election_night",
      contexts: [makeContext()],
    });

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("incompatible with pass_type");
  });

  it("rejects invalid roster candidate ids", () => {
    const parsed = parseElectionResultPayload(
      validOfficePayload({
        winners: [
          {
            candidate_election_id: "10000000-0000-0000-0000-000000000999",
            candidate_name: "Jane Candidate",
          },
        ],
      }),
      {
        passType: "election_night",
        contexts: [makeContext()],
      }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("not in provided roster");
  });

  it("ignores AI-returned candidate_id and derives the roster candidate_id", () => {
    const parsed = parseElectionResultPayload(
      validOfficePayload({
        winners: [
          {
            candidate_election_id: CANDIDATE_ELECTION_ID,
            candidate_id: "20000000-0000-0000-0000-000000000999",
          },
        ],
      }),
      {
        passType: "election_night",
        contexts: [makeContext()],
      }
    );

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.results[0]?.winners[0]?.candidate_id).toBe(CANDIDATE_ID);
      expect(parsed.payload.results[0]?.winners[0]?.candidate_name).toBe("Jane Candidate");
    }
  });

  it("requires winner name when no candidate_election_id is provided", () => {
    const parsed = parseElectionResultPayload(
      validOfficePayload({
        winners: [
          {
            party: "Democratic",
          },
        ],
      }),
      {
        passType: "election_night",
        contexts: [makeContext()],
      }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("candidate_name must be non-empty");
  });

  it("parses ballot measure results without winners", () => {
    const parsed = parseElectionResultPayload(
      {
        results: [
          {
            election_id: ELECTION_ID,
            result_status: "certified",
            outcome: "passed",
            winners: [],
            source_url: "https://elections.example.gov/prop-4-results",
            source_type: "official",
            notes: "",
          },
        ],
      },
      {
        passType: "certified",
        contexts: [
          makeContext({
            raceType: "ballot_measure",
            candidates: [],
            ballotMeasure: {
              ballotMeasureId: "30000000-0000-0000-0000-000000000001",
              officialBallotTitle: "Proposition 4",
              summary: null,
              whatYesMeans: null,
              whatNoMeans: null,
              result: null,
              sourceUrls: [],
              officialMeasureUrl: null,
            },
          }),
        ],
      }
    );

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.results[0]?.match_status).toBe("not_applicable");
    }
  });

  it("requires not_final_yet to use unknown outcome and no winners", () => {
    const parsed = parseElectionResultPayload(
      validOfficePayload({
        result_status: "not_final_yet",
        outcome: "won",
        winners: [],
      }),
      {
        passType: "certified",
        contexts: [makeContext()],
      }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("outcome=unknown");
  });
});
