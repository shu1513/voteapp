import { describe, expect, it, vi } from "vitest";

import { writeElectionResultPayload } from "../../src/pipeline/electionResults/electionResultWriter.js";
import type { ElectionResultContext } from "../../src/pipeline/electionResults/electionResultContextLoader.js";
import type { ElectionResultPayload } from "../../src/contracts/electionResultPayloadContract.js";

// Version/variant-valid UUIDs: the writer now fans decisive results out to
// notification events, whose creator rejects ids that fail the app's strict
// isUuid check (version nibble 1-5), as production ids always pass it.
const ELECTION_ID = "00000000-0000-4000-8000-000000000001";
const CANDIDATE_ELECTION_ID = "10000000-0000-4000-8000-000000000001";
const CANDIDATE_ID = "20000000-0000-4000-8000-000000000001";
const BALLOT_MEASURE_ID = "30000000-0000-4000-8000-000000000001";

function makeClient() {
  return {
    query: vi.fn(async (sql: string) => {
      if (String(sql).includes("INSERT INTO public.election_result_runs")) {
        return { rowCount: 1, rows: [{ id: "run-1" }] };
      }
      if (String(sql).includes("election_night_results_attempt_count = election_night_results_attempt_count + 1")) {
        return {
          rowCount: 1,
          rows: [{ id: ELECTION_ID, election_night_results_attempt_count: 1 }],
        };
      }
      if (String(sql).includes("certified_results_attempt_count = certified_results_attempt_count + 1")) {
        return {
          rowCount: 1,
          rows: [{ id: ELECTION_ID, certified_results_attempt_count: 1 }],
        };
      }
      return { rowCount: 1, rows: [] };
    }),
    release: vi.fn(),
  };
}

function officeContext(): ElectionResultContext {
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

function ballotMeasureContext(): ElectionResultContext {
  return {
    ...officeContext(),
    raceType: "ballot_measure",
    officialBallotTitle: "Proposition 4",
    candidates: [],
    ballotMeasure: {
      ballotMeasureId: BALLOT_MEASURE_ID,
      officialBallotTitle: "Proposition 4",
      summary: null,
      whatYesMeans: null,
      whatNoMeans: null,
      result: null,
      sourceUrls: [],
      officialMeasureUrl: null,
    },
  };
}

function officePayload(overrides: Partial<ElectionResultPayload["results"][number]> = {}): ElectionResultPayload {
  return {
    results: [
      {
        election_id: ELECTION_ID,
        result_status: "unofficial",
        outcome: "won",
        winners: [
          {
            candidate_election_id: CANDIDATE_ELECTION_ID,
            candidate_id: CANDIDATE_ID,
            candidate_name: "Jane Candidate",
          },
        ],
        match_status: "matched",
        source_url: "https://elections.example.gov/results",
        source_type: "official",
        notes: "",
        ...overrides,
      },
    ],
  };
}

describe("writeElectionResultPayload", () => {
  it("stores election-night results without updating canonical candidate status", async () => {
    const client = makeClient();

    const result = await writeElectionResultPayload(client as never, {
      state: "CA",
      electionDate: "2026-11-03",
      passType: "election_night",
      contexts: [officeContext()],
      payload: officePayload(),
      provider: "claude",
      model: "claude-sonnet-4-6",
      checkedAt: new Date("2026-11-04T04:10:00.000Z"),
    });

    expect(result.electionRowsWritten).toBe(1);
    expect(result.canonicalCandidateStatusUpdates).toBe(0);
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining("INSERT INTO public.election_results"),
      expect.any(Array)
    );
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("UPDATE public.candidate_elections"))
    ).toBe(false);
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("election_night_results_checked_at"))
    ).toBe(true);
  });

  it("projects certified matched official office results to candidate_elections status", async () => {
    const client = makeClient();

    const result = await writeElectionResultPayload(client as never, {
      state: "CA",
      electionDate: "2026-11-03",
      passType: "certified",
      contexts: [officeContext()],
      payload: officePayload({ result_status: "certified" }),
      provider: "claude",
      model: "claude-sonnet-4-6",
      checkedAt: new Date("2026-12-10T18:00:00.000Z"),
      sourceVerifications: [
        {
          sourceUrl: "https://elections.example.gov/results",
          finalUrl: "https://elections.example.gov/results",
          status: 200,
          authority: "verified",
        },
      ],
    });

    expect(result.canonicalCandidateStatusUpdates).toBe(1);
    const statusUpdate = client.query.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.candidate_elections")
    );
    expect(statusUpdate?.[1]).toEqual([ELECTION_ID, [CANDIDATE_ELECTION_ID], "won"]);
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("certified_results_checked_at"))
    ).toBe(true);
  });

  it("does not project certified office results from weak source verification", async () => {
    const client = makeClient();

    const result = await writeElectionResultPayload(client as never, {
      state: "CA",
      electionDate: "2026-11-03",
      passType: "certified",
      contexts: [officeContext()],
      payload: officePayload({ result_status: "certified" }),
      provider: "claude",
      model: "claude-sonnet-4-6",
      sourceVerifications: [
        {
          sourceUrl: "https://elections.example.gov/results",
          finalUrl: "https://elections.example.gov/results",
          status: 403,
          authority: "weak",
        },
      ],
    });

    expect(result.canonicalCandidateStatusUpdates).toBe(0);
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("UPDATE public.candidate_elections"))
    ).toBe(false);
  });

  it("does not project certified office results when source verification is missing", async () => {
    const client = makeClient();

    const result = await writeElectionResultPayload(client as never, {
      state: "CA",
      electionDate: "2026-11-03",
      passType: "certified",
      contexts: [officeContext()],
      payload: officePayload({ result_status: "certified" }),
      provider: "claude",
      model: "claude-sonnet-4-6",
    });

    expect(result.canonicalCandidateStatusUpdates).toBe(0);
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("UPDATE public.candidate_elections"))
    ).toBe(false);
  });

  it("does not mark certified office result checked while winners are unmatched", async () => {
    const client = makeClient();

    const result = await writeElectionResultPayload(client as never, {
      state: "CA",
      electionDate: "2026-11-03",
      passType: "certified",
      contexts: [officeContext()],
      payload: officePayload({
        result_status: "certified",
        winners: [{ candidate_name: "Pat Connolly" }],
        match_status: "unmatched",
      }),
      provider: "claude",
      model: "claude-sonnet-4-6",
      sourceVerifications: [
        {
          sourceUrl: "https://elections.example.gov/results",
          finalUrl: "https://elections.example.gov/results",
          status: 200,
          authority: "verified",
        },
      ],
    });

    expect(result.checkedElectionCount).toBe(0);
    expect(result.uncheckedElectionIds).toEqual([ELECTION_ID]);
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("certified_results_checked_at"))
    ).toBe(false);
  });

  it("marks certified office result checked on the third unresolved certified attempt", async () => {
    const client = makeClient();
    client.query.mockImplementation(async (sql: string) => {
      const text = String(sql);
      if (text.includes("INSERT INTO public.election_result_runs")) {
        return { rowCount: 1, rows: [{ id: "run-1" }] };
      }
      if (text.includes("certified_results_attempt_count = certified_results_attempt_count + 1")) {
        return {
          rowCount: 1,
          rows: [{ id: ELECTION_ID, certified_results_attempt_count: 3 }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    const result = await writeElectionResultPayload(client as never, {
      state: "CA",
      electionDate: "2026-11-03",
      passType: "certified",
      contexts: [officeContext()],
      payload: officePayload({
        result_status: "not_final_yet",
        outcome: "unknown",
        winners: [{ candidate_name: "Pat Connolly" }],
        match_status: "unmatched",
      }),
      provider: "claude",
      model: "claude-sonnet-4-6",
      sourceVerifications: [
        {
          sourceUrl: "https://elections.example.gov/results",
          finalUrl: "https://elections.example.gov/results",
          status: 200,
          authority: "verified",
        },
      ],
      checkedAt: new Date("2026-12-31T18:00:00.000Z"),
    });

    expect(result.checkedElectionCount).toBe(1);
    expect(result.uncheckedElectionIds).toEqual([]);
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("certified_results_checked_at"))
    ).toBe(true);
  });

  it("projects certified ballot measure passed/failed outcomes to ballot_measures.result", async () => {
    const client = makeClient();

    const result = await writeElectionResultPayload(client as never, {
      state: "CA",
      electionDate: "2026-11-03",
      passType: "certified",
      contexts: [ballotMeasureContext()],
      payload: {
        results: [
          {
            election_id: ELECTION_ID,
            result_status: "certified",
            outcome: "passed",
            winners: [],
            match_status: "not_applicable",
            source_url: "https://elections.example.gov/prop4-results",
            source_type: "official",
            notes: "",
          },
        ],
      },
      provider: "claude",
      model: "claude-sonnet-4-6",
      sourceVerifications: [
        {
          sourceUrl: "https://elections.example.gov/prop4-results",
          finalUrl: "https://elections.example.gov/prop4-results",
          status: 200,
          authority: "verified",
        },
      ],
    });

    expect(result.ballotMeasureRowsWritten).toBe(1);
    expect(result.canonicalBallotMeasureUpdates).toBe(1);
    expect(
      client.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.ballot_measure_results"))
    ).toBe(true);
    const resultUpdate = client.query.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.ballot_measures")
    );
    expect(resultUpdate?.[1]).toEqual([BALLOT_MEASURE_ID, "passed"]);
  });
});
