import { beforeEach, describe, expect, it, vi } from "vitest";

const ELECTION_ID = "00000000-0000-0000-0000-000000000999";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});

const redisConnectMock = vi.fn(async () => {});
const redisQuitMock = vi.fn(async () => {});
const redisXGroupCreateMock = vi.fn(async () => "OK");
const redisXAutoClaimMock = vi.fn(async () => ({ nextId: "0-0", messages: [] }));
const redisXReadGroupMock = vi.fn();
const redisXAckMock = vi.fn(async () => 1);
const redisSendCommandMock = vi.fn(async () => 1);

const enrichCandidateRosterMock = vi.fn();

function installCandidateRosterEnricherMocks(): void {
  vi.doMock("pg", () => ({
    Pool: vi.fn(() => ({
      query: poolQueryMock,
      end: poolEndMock,
    })),
  }));

  vi.doMock("redis", () => ({
    createClient: vi.fn(() => ({
      connect: redisConnectMock,
      quit: redisQuitMock,
      xGroupCreate: redisXGroupCreateMock,
      xAutoClaim: redisXAutoClaimMock,
      xReadGroup: redisXReadGroupMock,
      xAck: redisXAckMock,
      sendCommand: redisSendCommandMock,
    })),
  }));

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

  vi.doMock("../../src/ai/enrichCandidateRoster.js", () => ({
    buildCandidateRosterConfigFromEnv: () => ({}),
    enrichCandidateRoster: enrichCandidateRosterMock,
    disambiguateCandidateDuplicateGroup: vi.fn(),
  }));
}

function mockPoolForElection(options: { electionExists?: boolean; stagingStatus?: string; stagingPayload?: unknown } = {}): void {
  const { electionExists = true, stagingStatus = "pending", stagingPayload = { election_id: ELECTION_ID } } = options;
  poolQueryMock.mockImplementation(async (sql: string) => {
    if (sql.includes("FROM public.elections AS e")) {
      if (!electionExists) {
        return { rows: [] };
      }
      return {
        rows: [
          {
            id: ELECTION_ID,
            district_name: "Vermont",
            district_type: "statewide",
            state: "VT",
            election_date: "2026-08-11",
            official_ballot_title: "Governor",
            election_stage: "primary",
            senate_class: null,
            term_end_year: null,
            is_partisan: true,
            sources: [],
          },
        ],
      };
    }

    if (sql.includes("SELECT ingest_key, payload, status, run_id")) {
      return {
        rows: [
          {
            ingest_key: `candidate_roster:${ELECTION_ID}`,
            payload: stagingPayload,
            status: stagingStatus,
            run_id: "run_1",
          },
        ],
      };
    }

    if (sql.includes("FROM public.candidate_elections AS ce")) {
      return { rows: [] };
    }

    return { rows: [] };
  });
}

describe("runCandidateRosterEnricherForElection", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env.CANDIDATE_ROSTER_ENABLE_ENRICHER_ELIGIBILITY_GATE = "false";
  });

  it("enriches one election without ever touching the shared draft stream", async () => {
    installCandidateRosterEnricherMocks();
    mockPoolForElection();

    enrichCandidateRosterMock.mockResolvedValue({
      ok: true,
      candidates: [
        {
          display_name: "Casey Example",
          party: "Democratic",
          sources: ["https://example.org/candidate"],
        },
      ],
      aiRawDebug: { provider: "test" },
    });

    const { runCandidateRosterEnricherForElection } = await import(
      "../../src/pipeline/enrichers/candidateRosterEnricher.js"
    );

    const result = await runCandidateRosterEnricherForElection(ELECTION_ID);

    expect(result).toEqual({ outcome: "written", candidateCount: 1, rosterSource: "ai", runId: "run_1" });

    const sqlStatements = poolQueryMock.mock.calls.map(([sql]) => String(sql));
    expect(sqlStatements.some((sql) => sql.includes("status = 'validated'"))).toBe(true);
    expect(sqlStatements.some((sql) => sql.includes("status = 'written'"))).toBe(true);
    // Fanout reached Redis via the profile-draft emitter, carrying the
    // staging row's run_id even though the targeted entrypoint has none of
    // its own — the emitter's marker dedupe means these drafts are the only
    // chance to attach the right correlation id.
    expect(redisSendCommandMock).toHaveBeenCalled();
    const emitArgs = redisSendCommandMock.mock.calls[0]![0] as string[];
    expect(emitArgs).toContain("run_1");

    // The whole point of targeted mode: the shared stream is never consumed,
    // so other elections' messages cannot be read, acked, or parked.
    expect(redisXGroupCreateMock).not.toHaveBeenCalled();
    expect(redisXReadGroupMock).not.toHaveBeenCalled();
    expect(redisXAutoClaimMock).not.toHaveBeenCalled();
    expect(redisXAckMock).not.toHaveBeenCalled();
  });

  it("marks no_results when the AI finds zero candidates", async () => {
    installCandidateRosterEnricherMocks();
    mockPoolForElection();

    enrichCandidateRosterMock.mockResolvedValue({
      ok: true,
      candidates: [],
      aiRawDebug: { provider: "test" },
    });

    const { runCandidateRosterEnricherForElection } = await import(
      "../../src/pipeline/enrichers/candidateRosterEnricher.js"
    );

    const result = await runCandidateRosterEnricherForElection(ELECTION_ID);

    expect(result).toEqual({ outcome: "no_results" });
    const sqlStatements = poolQueryMock.mock.calls.map(([sql]) => String(sql));
    expect(sqlStatements.some((sql) => sql.includes("status = 'no_results'"))).toBe(true);
    expect(sqlStatements.some((sql) => sql.includes("status = 'written'"))).toBe(false);
  });

  it("fans out from the staged payload without an AI call when already validated", async () => {
    installCandidateRosterEnricherMocks();
    mockPoolForElection({
      stagingStatus: "validated",
      stagingPayload: {
        election_id: ELECTION_ID,
        candidates: [
          {
            display_name: "Casey Example",
            party: "Democratic",
            sources: ["https://example.org/candidate"],
            roster_index: 0,
          },
        ],
      },
    });

    const { runCandidateRosterEnricherForElection } = await import(
      "../../src/pipeline/enrichers/candidateRosterEnricher.js"
    );

    const result = await runCandidateRosterEnricherForElection(ELECTION_ID);

    expect(result).toEqual({ outcome: "written", candidateCount: 1, rosterSource: "staged_payload", runId: "run_1" });
    expect(enrichCandidateRosterMock).not.toHaveBeenCalled();
    expect(redisSendCommandMock).toHaveBeenCalled();
  });

  it("throws on an unknown election id without creating a staging row", async () => {
    installCandidateRosterEnricherMocks();
    mockPoolForElection({ electionExists: false });

    const { runCandidateRosterEnricherForElection } = await import(
      "../../src/pipeline/enrichers/candidateRosterEnricher.js"
    );

    await expect(runCandidateRosterEnricherForElection(ELECTION_ID)).rejects.toThrow(
      /election not found/
    );

    // The election lookup runs before the staging insert, so a typo id never
    // leaves a junk pending staging row behind.
    const sqlStatements = poolQueryMock.mock.calls.map(([sql]) => String(sql));
    expect(sqlStatements.some((sql) => sql.includes("INSERT INTO staging_items"))).toBe(false);
    expect(enrichCandidateRosterMock).not.toHaveBeenCalled();
  });

  it("records the AI failure on the staging row and throws", async () => {
    installCandidateRosterEnricherMocks();
    mockPoolForElection();

    enrichCandidateRosterMock.mockResolvedValue({
      ok: false,
      errorCode: "timeout",
      reason: "provider timed out",
      failureDebug: { provider: "test" },
    });

    const { runCandidateRosterEnricherForElection } = await import(
      "../../src/pipeline/enrichers/candidateRosterEnricher.js"
    );

    await expect(runCandidateRosterEnricherForElection(ELECTION_ID)).rejects.toThrow(
      /ai failure \(timeout\): provider timed out/
    );

    const sqlStatements = poolQueryMock.mock.calls.map(([sql]) => String(sql));
    expect(sqlStatements.some((sql) => sql.includes("SET reason = $2"))).toBe(true);
    expect(sqlStatements.some((sql) => sql.includes("status = 'written'"))).toBe(false);
    // Connections still close on the failure path.
    expect(redisQuitMock).toHaveBeenCalled();
    expect(poolEndMock).toHaveBeenCalled();
  });
});
