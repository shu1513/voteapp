import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});

const redisConnectMock = vi.fn(async () => {});
const redisQuitMock = vi.fn(async () => {});
const redisXGroupCreateMock = vi.fn(async () => "OK");
const redisXAutoClaimMock = vi.fn(async () => ({ nextId: "0-0", messages: [] }));
const redisXReadGroupMock = vi.fn();
const redisXAckMock = vi.fn(async () => 1);
const redisSendCommandMock = vi.fn(async () => []);

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

describe("runCandidateRosterEnricher empty roster", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env.CANDIDATE_ROSTER_ENABLE_ENRICHER_ELIGIBILITY_GATE = "false";

    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:roster:draft",
        messages: [
          {
            id: "1-0",
            message: {
              election_id: "00000000-0000-0000-0000-000000000999",
              item_type: "candidate_roster",
              run_id: "run_1",
            },
          },
        ],
      },
    ]);

    poolQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("SELECT ingest_key, payload, status, run_id")) {
        return {
          rows: [
            {
              ingest_key: "candidate_roster:00000000-0000-0000-0000-000000000999",
              payload: { election_id: "00000000-0000-0000-0000-000000000999" },
              status: "pending",
              run_id: "run_1",
            },
          ],
        };
      }

      if (sql.includes("FROM public.elections AS e")) {
        return {
          rows: [
            {
              id: "00000000-0000-0000-0000-000000000999",
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

      if (sql.includes("FROM public.candidate_elections AS ce")) {
        return { rows: [] };
      }

      return { rows: [] };
    });
  });

  it("marks the staging row no_results instead of written when the AI finds zero candidates", async () => {
    installCandidateRosterEnricherMocks();

    enrichCandidateRosterMock.mockResolvedValue({
      ok: true,
      candidates: [],
      aiRawDebug: { provider: "test" },
    });

    const { runCandidateRosterEnricher } = await import(
      "../../src/pipeline/enrichers/candidateRosterEnricher.js"
    );

    await runCandidateRosterEnricher({ once: true, batchSize: 5, blockMs: 10 });

    const sqlStatements = poolQueryMock.mock.calls.map(([sql]) => String(sql));
    // A frozen empty roster is the bug: 'written' trips the eligibility
    // gate's already_written check forever, so zero candidates must land as
    // the retryable 'no_results' and never as 'validated'/'written'.
    expect(sqlStatements.some((sql) => sql.includes("status = 'no_results'"))).toBe(true);
    expect(sqlStatements.some((sql) => sql.includes("status = 'written'"))).toBe(false);
    expect(sqlStatements.some((sql) => sql.includes("status = 'validated'"))).toBe(false);
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:roster:draft",
      "candidate_roster_enricher",
      "1-0"
    );
  });

  it("still writes the roster when candidates were found", async () => {
    installCandidateRosterEnricherMocks();

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

    const { runCandidateRosterEnricher } = await import(
      "../../src/pipeline/enrichers/candidateRosterEnricher.js"
    );

    await runCandidateRosterEnricher({ once: true, batchSize: 5, blockMs: 10 });

    const sqlStatements = poolQueryMock.mock.calls.map(([sql]) => String(sql));
    expect(sqlStatements.some((sql) => sql.includes("status = 'no_results'"))).toBe(false);
    expect(sqlStatements.some((sql) => sql.includes("status = 'written'"))).toBe(true);
  });
});
