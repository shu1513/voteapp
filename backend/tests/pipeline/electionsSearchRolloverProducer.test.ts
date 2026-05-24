import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});
const redisConnectMock = vi.fn(async () => {});
const redisQuitMock = vi.fn(async () => {});
const redisXAddMock = vi.fn(async () => "1-0");

describe("runElectionsSearchRolloverProducer", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env.ELECTIONS_SEARCH_ROLLOVER_ENABLED = "true";
    process.env.ELECTIONS_RESEARCH_COOLDOWN_DAYS = "180";
    process.env.ELECTIONS_SEARCH_MAX_ENQUEUE_PER_RUN = "2";
  });

  it("enqueues only due districts", async () => {
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
        xAdd: redisXAddMock,
        isOpen: true,
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

    vi.doMock("../../src/pipeline/elections/electionsSearchEligibility.js", () => ({
      listDistrictElectionSearchEligibility: vi.fn(async () => [
        {
          district_id: "d-never",
          district_name: "Never",
          district_type: "county",
          state: "CA",
          last_elections_searched_at: null,
          max_known_election_date: null,
          has_upcoming: false,
          reason: "never_searched",
        },
        {
          district_id: "d-past",
          district_name: "Past",
          district_type: "county",
          state: "CA",
          last_elections_searched_at: "2025-01-01T00:00:00.000Z",
          max_known_election_date: "2026-01-01",
          has_upcoming: false,
          reason: "due_no_upcoming",
        },
        {
          district_id: "d-cooldown",
          district_name: "Cooldown",
          district_type: "county",
          state: "CA",
          last_elections_searched_at: "2026-05-01T00:00:00.000Z",
          max_known_election_date: "2026-11-03",
          has_upcoming: true,
          reason: "cooldown_not_elapsed",
        },
      ]),
    }));

    poolQueryMock.mockResolvedValue({ rowCount: 1, rows: [{ ingest_key: "ok" }] });

    const { runElectionsSearchRolloverProducer } = await import(
      "../../src/pipeline/producers/electionsSearchRolloverProducer.js"
    );
    const result = await runElectionsSearchRolloverProducer();

    expect(result.districts_scanned).toBe(3);
    expect(result.due_count).toBe(2);
    expect(result.due_overflow_count).toBe(0);
    expect(result.enqueued_count).toBe(2);
    expect(result.skipped_cooldown).toBe(1);
    expect(redisXAddMock).toHaveBeenCalledTimes(2);
  });

  it("respects max enqueue cap", async () => {
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
        xAdd: redisXAddMock,
        isOpen: true,
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

    vi.doMock("../../src/pipeline/elections/electionsSearchEligibility.js", () => ({
      listDistrictElectionSearchEligibility: vi.fn(async () => [
        {
          district_id: "d1",
          district_name: "D1",
          district_type: "county",
          state: "CA",
          last_elections_searched_at: null,
          max_known_election_date: null,
          has_upcoming: false,
          reason: "never_searched",
        },
        {
          district_id: "d2",
          district_name: "D2",
          district_type: "county",
          state: "CA",
          last_elections_searched_at: null,
          max_known_election_date: null,
          has_upcoming: false,
          reason: "due_no_upcoming",
        },
        {
          district_id: "d3",
          district_name: "D3",
          district_type: "county",
          state: "CA",
          last_elections_searched_at: null,
          max_known_election_date: null,
          has_upcoming: false,
          reason: "due_no_upcoming",
        },
      ]),
    }));

    poolQueryMock.mockResolvedValue({ rowCount: 1, rows: [{ ingest_key: "ok" }] });

    const { runElectionsSearchRolloverProducer } = await import(
      "../../src/pipeline/producers/electionsSearchRolloverProducer.js"
    );
    const result = await runElectionsSearchRolloverProducer({ maxEnqueuePerRun: 2 });

    expect(result.due_count).toBe(3);
    expect(result.due_overflow_count).toBe(1);
    expect(result.max_enqueue_hit).toBe(true);
    expect(result.enqueued_count).toBe(2);
    expect(redisXAddMock).toHaveBeenCalledTimes(2);
  });
});
