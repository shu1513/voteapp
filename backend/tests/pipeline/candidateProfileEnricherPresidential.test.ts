import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.hoisted(() => vi.fn());
const poolConnectMock = vi.hoisted(() => vi.fn());
const poolEndMock = vi.hoisted(() => vi.fn());
const clientQueryMock = vi.hoisted(() => vi.fn());
const clientReleaseMock = vi.hoisted(() => vi.fn());
const redisConnectMock = vi.hoisted(() => vi.fn());
const redisQuitMock = vi.hoisted(() => vi.fn());
const redisXGroupCreateMock = vi.hoisted(() => vi.fn());
const redisXAutoClaimMock = vi.hoisted(() => vi.fn());
const redisSendCommandMock = vi.hoisted(() => vi.fn());
const redisXReadGroupMock = vi.hoisted(() => vi.fn());
const redisXAckMock = vi.hoisted(() => vi.fn());
const redisXAddMock = vi.hoisted(() => vi.fn());
const enrichCandidateProfileMock = vi.hoisted(() => vi.fn());
const enqueueCandidateRecordDraftsMock = vi.hoisted(() => vi.fn());

vi.mock("pg", () => ({
  Pool: vi.fn(() => ({
    query: poolQueryMock,
    connect: poolConnectMock,
    end: poolEndMock,
  })),
}));

vi.mock("redis", () => ({
  createClient: vi.fn(() => ({
    connect: redisConnectMock,
    quit: redisQuitMock,
    xGroupCreate: redisXGroupCreateMock,
    xAutoClaim: redisXAutoClaimMock,
    sendCommand: redisSendCommandMock,
    xReadGroup: redisXReadGroupMock,
    xAck: redisXAckMock,
    xAdd: redisXAddMock,
  })),
}));

vi.mock("../../src/config/env.js", () => ({
  getPipelineEnv: () => ({
    DATABASE_URL: "postgres://test/test",
    REDIS_URL: "redis://localhost:6379/0",
    AI_TIMEOUT_MS: 1000,
    ANTHROPIC_WEB_SEARCH_MAX_USES: 1,
  }),
}));

vi.mock("../../src/ai/enrichCandidateProfile.js", () => ({
  buildCandidateProfileConfigFromEnv: vi.fn(() => ({ timeoutMs: 1000 })),
  enrichCandidateProfile: enrichCandidateProfileMock,
}));

vi.mock("../../src/pipeline/candidates/candidateRecordDraftEmitter.js", () => ({
  enqueueCandidateRecordDrafts: enqueueCandidateRecordDraftsMock,
}));

import { runCandidateProfileEnricher } from "../../src/pipeline/enrichers/candidateProfileEnricher.js";

describe("runCandidateProfileEnricher presidential cycle routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    redisConnectMock.mockResolvedValue(undefined);
    redisQuitMock.mockResolvedValue(undefined);
    redisXGroupCreateMock.mockResolvedValue(undefined);
    redisXAutoClaimMock.mockResolvedValue({ nextId: "0-0", messages: [] });
    redisSendCommandMock.mockResolvedValue([]);
    redisXAckMock.mockResolvedValue(1);
    redisXAddMock.mockResolvedValue("2-0");
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-0",
            message: {
              context_type: "presidential_cycle",
              presidential_cycle_id: "cycle-1",
              item_type: "candidate_profile",
              candidate_display_name: "Jane President",
              roster_party: "Republican",
              roster_fec_ids: JSON.stringify(["P80000001"]),
              seed_urls: JSON.stringify(["https://www.fec.gov/data/candidate/P80000001"]),
              run_id: "run-1",
            },
          },
        ],
      },
    ]);

    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (String(sql).includes("FROM public.presidential_cycles")) {
        expect(params).toEqual(["cycle-1"]);
        return {
          rows: [
            {
              id: "cycle-1",
              election_year: 2028,
              stage: "primary",
              party: "Democratic",
              election_date: null,
              sources: ["https://example.gov/primary"],
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });

    poolConnectMock.mockResolvedValue({ query: clientQueryMock, release: clientReleaseMock });
    poolEndMock.mockResolvedValue(undefined);
    clientReleaseMock.mockReturnValue(undefined);
    clientQueryMock.mockImplementation(async (sql: string) => {
      const text = String(sql);
      if (text === "BEGIN" || text === "COMMIT" || text === "ROLLBACK") {
        return { rows: [], rowCount: null };
      }
      if (text.includes("FROM public.candidates")) {
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("INSERT INTO public.candidates")) {
        return { rows: [{ id: "candidate-1" }], rowCount: 1 };
      }
      if (text.includes("INSERT INTO public.presidential_cycle_candidates")) {
        return { rows: [], rowCount: 1 };
      }
      if (text.includes("INSERT INTO public.candidate_elections")) {
        return { rows: [], rowCount: 1 };
      }
      throw new Error(`Unexpected client query: ${text}`);
    });

    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane President",
        first_name: "Jane",
        last_name: "President",
        party: "Democratic",
        fec_ids: ["P80000001"],
        sources: ["https://www.fec.gov/data/candidate/P80000001"],
      },
    });
  });

  it("links presidential profile drafts to presidential cycle candidates only", async () => {
    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enrichCandidateProfileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Jane President",
        districtName: "United States",
        districtType: "presidential",
        state: "US",
        electionDate: null,
        officialBallotTitle: "President of the United States, 2028 Democratic primary",
        electionStage: "primary",
        rosterParty: "Democratic",
        rosterFecIds: ["P80000001"],
        seedUrls: ["https://www.fec.gov/data/candidate/P80000001", "https://example.gov/primary"],
      }),
      { timeoutMs: 1000 }
    );

    const executedSql = clientQueryMock.mock.calls.map((call) => String(call[0]));
    const candidateLookupSql = executedSql.find((sql) => sql.includes("FROM public.candidates"));
    expect(candidateLookupSql).not.toContain("AND state = $3");
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.presidential_cycle_candidates"))).toBe(true);
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.candidate_elections"))).toBe(false);
    const presidentialUpsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.presidential_cycle_candidates")
    );
    expect(presidentialUpsertCall?.[1]).toEqual([
      "cycle-1",
      "candidate-1",
      "Democratic",
      "active",
      JSON.stringify(["https://www.fec.gov/data/candidate/P80000001"]),
    ]);
    expect(enqueueCandidateRecordDraftsMock).not.toHaveBeenCalled();
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-0"
    );
  });

  it("preserves election profile drafts as candidate election links with record drafts", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-1",
            message: {
              election_id: "election-1",
              item_type: "candidate_profile",
              candidate_display_name: "Jane Candidate",
              roster_party: "Democratic",
              roster_is_incumbent: "true",
              roster_fec_ids: JSON.stringify(["P80000002"]),
              seed_urls: JSON.stringify(["https://example.gov/candidate"]),
              run_id: "run-election",
            },
          },
        ],
      },
    ]);
    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      const text = String(sql);
      if (text.includes("FROM public.candidate_elections AS ce")) {
        expect(params).toEqual(["election-1"]);
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("FROM public.elections AS e")) {
        expect(params).toEqual(["election-1"]);
        return {
          rows: [
            {
              id: "election-1",
              state: "CA",
              district_name: "California",
              district_type: "statewide",
              election_date: "2028-11-07",
              official_ballot_title: "Governor",
              election_stage: "general",
              senate_class: null,
              term_end_year: null,
              is_partisan: true,
              sources: ["https://example.gov/election"],
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });
    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        party: "Democratic",
        fec_ids: ["P80000002"],
        sources: ["https://example.gov/candidate"],
      },
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enrichCandidateProfileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Jane Candidate",
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2028-11-07",
        officialBallotTitle: "Governor",
        electionStage: "general",
        rosterParty: "Democratic",
        rosterIncumbent: true,
        rosterFecIds: ["P80000002"],
        seedUrls: ["https://example.gov/candidate", "https://example.gov/election"],
      }),
      { timeoutMs: 1000 }
    );

    const executedSql = clientQueryMock.mock.calls.map((call) => String(call[0]));
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.candidate_elections"))).toBe(true);
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.presidential_cycle_candidates"))).toBe(false);
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        runId: "run-election",
      },
    ]);
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-1"
    );
  });
});
