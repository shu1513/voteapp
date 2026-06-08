import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ElectionResultPayload } from "../../src/contracts/electionResultPayloadContract.js";
import type { ElectionResultContext } from "../../src/pipeline/electionResults/electionResultContextLoader.js";

const ELECTION_ID = "00000000-0000-0000-0000-000000000001";
const CANDIDATE_ELECTION_ID = "10000000-0000-0000-0000-000000000001";
const CANDIDATE_ID = "20000000-0000-0000-0000-000000000001";

const poolQueryMock = vi.fn();
const poolConnectMock = vi.fn();
const poolEndMock = vi.fn(async () => {});
const clientQueryMock = vi.fn();
const clientReleaseMock = vi.fn();

function officeContext(overrides: Partial<ElectionResultContext> = {}): ElectionResultContext {
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

function officePayload(): ElectionResultPayload {
  return {
    results: [
      {
        election_id: ELECTION_ID,
        result_status: "certified",
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
      },
    ],
  };
}

function unmatchedOfficePayload(): ElectionResultPayload {
  return {
    results: [
      {
        election_id: ELECTION_ID,
        result_status: "certified",
        outcome: "won",
        winners: [{ candidate_name: "Pat Connolly", party: "Nonpartisan" }],
        match_status: "unmatched",
        source_url: "https://elections.example.gov/results",
        source_type: "official",
        notes: "",
      },
    ],
  };
}

describe("processElectionResultSearchJob", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();

    poolQueryMock.mockImplementation(async (sql: string) => {
      const text = String(sql);
      if (text.includes("INSERT INTO public.election_result_runs")) {
        return { rowCount: 1, rows: [{ id: "run-db-1" }] };
      }
      if (text.includes("UPDATE public.election_result_runs")) {
        return { rowCount: 1, rows: [] };
      }
      return { rowCount: 1, rows: [] };
    });
    poolConnectMock.mockResolvedValue({
      query: clientQueryMock,
      release: clientReleaseMock,
    });
    clientQueryMock.mockImplementation(async (sql: string) => {
      const text = String(sql);
      if (text.includes("INSERT INTO public.election_results")) {
        throw new Error("result insert failed");
      }
      return { rowCount: 1, rows: [] };
    });
  });

  it("persists failed run bookkeeping outside the rolled-back result transaction", async () => {
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));

    vi.doMock("../../src/ai/enrichElectionResults.js", () => ({
      buildElectionResultAiConfigFromEnv: () => ({
        timeoutMs: 90000,
      }),
      enrichElectionResults: vi.fn(async () => ({
        ok: true,
        payload: officePayload(),
        provider: "claude",
        model: "claude-sonnet-4-6",
        sourceVerifications: [],
        aiRawDebug: null,
      })),
    }));

    vi.doMock("../../src/pipeline/electionResults/electionResultContextLoader.js", () => ({
      loadElectionResultContexts: vi.fn(async () => [officeContext()]),
      chunkElectionResultContexts: vi.fn((contexts: ElectionResultContext[]) => [contexts]),
    }));

    const { processElectionResultSearchJob } = await import(
      "../../src/pipeline/enrichers/electionResultsEnricher.js"
    );

    const pool = {
      query: poolQueryMock,
      connect: poolConnectMock,
      end: poolEndMock,
    };

    await expect(
      processElectionResultSearchJob(
        {
          state: "CA",
          election_date: "2026-11-03",
          pass_type: "certified",
          scheduled_for: "2026-12-10T18:00:00.000Z",
          election_ids: [ELECTION_ID],
          run_id: "scheduled-run-1",
        },
        { pool: pool as never }
      )
    ).rejects.toThrow("result insert failed");

    expect(clientQueryMock).toHaveBeenCalledWith("BEGIN");
    expect(clientQueryMock).toHaveBeenCalledWith("ROLLBACK");
    expect(clientReleaseMock).toHaveBeenCalledTimes(1);

    const failedRunUpdate = poolQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.election_result_runs")
    );
    expect(failedRunUpdate?.[1]).toEqual([
      "run-db-1",
      "failed",
      JSON.stringify({
        provider: "claude",
        model: "claude-sonnet-4-6",
      }),
      JSON.stringify(officePayload()),
    ]);
  });

  it("emits candidate-profile drafts and clears certified markers for unmatched winners", async () => {
    clientQueryMock.mockImplementation(async () => ({ rowCount: 1, rows: [] }));

    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));

    vi.doMock("../../src/ai/enrichElectionResults.js", () => ({
      buildElectionResultAiConfigFromEnv: () => ({
        timeoutMs: 90000,
      }),
      enrichElectionResults: vi.fn(async () => ({
        ok: true,
        payload: unmatchedOfficePayload(),
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
        aiRawDebug: null,
      })),
    }));

    vi.doMock("../../src/pipeline/electionResults/electionResultContextLoader.js", () => ({
      loadElectionResultContexts: vi.fn(async () => [officeContext({ candidates: [] })]),
      chunkElectionResultContexts: vi.fn((contexts: ElectionResultContext[]) => [contexts]),
    }));

    const { processElectionResultSearchJob } = await import(
      "../../src/pipeline/enrichers/electionResultsEnricher.js"
    );

    const redisFanoutClient = {
      sendCommand: vi.fn(async () => 1),
      del: vi.fn(async () => 1),
    };
    const result = await processElectionResultSearchJob(
      {
        state: "CA",
        election_date: "2026-11-03",
        pass_type: "certified",
        scheduled_for: "2026-12-10T18:00:00.000Z",
        election_ids: [ELECTION_ID],
        run_id: "scheduled-run-1",
      },
      {
        pool: {
          query: poolQueryMock,
          connect: poolConnectMock,
          end: poolEndMock,
        } as never,
        redisFanoutClient,
      }
    );

    expect(result.candidate_profile_drafts_emitted).toBe(1);
    expect(result.checked_election_count).toBe(0);
    expect(redisFanoutClient.sendCommand).toHaveBeenCalledTimes(1);
    const args = redisFanoutClient.sendCommand.mock.calls[0]?.[0] as string[];
    expect(args[3]).toBe("staging:candidates:profile:draft");
    expect(args[4]).toBe(
      `staging:candidate_profile_draft_emitted:election_result_winner:${ELECTION_ID}:pat connolly`
    );
    expect(args[8]).toBe("Pat Connolly");
    expect(args[9]).toBe("Nonpartisan");
    expect(args[11]).toBe(JSON.stringify(["https://elections.example.gov/results"]));
    expect(args[13]).toBe("Winner/advancer reported by election result source for Governor.");
    expect(args[14]).toBe("false");
    expect(redisFanoutClient.del).toHaveBeenCalledWith(`staging:election_result_emitted:certified:${ELECTION_ID}`);
  });
});
