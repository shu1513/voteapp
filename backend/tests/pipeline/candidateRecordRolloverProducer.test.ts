import { beforeEach, describe, expect, it, vi } from "vitest";

const poolEndMock = vi.fn(async () => {});
const poolQueryMock = vi.fn();
const redisConnectMock = vi.fn(async () => {});
const redisQuitMock = vi.fn(async () => {});
const enqueueDraftsMock = vi.fn();

describe("runCandidateRecordRolloverProducer", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env.CANDIDATE_RECORD_ENABLE_DAILY_ROLLOVER_PRODUCER = "true";
    process.env.CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS = "30";
    process.env.CANDIDATE_RECORDS_ROLLOVER_MAX_ENQUEUE = "2";
  });

  it("returns disabled summary when feature flag is off", async () => {
    process.env.CANDIDATE_RECORD_ENABLE_DAILY_ROLLOVER_PRODUCER = "false";

    const { runCandidateRecordRolloverProducer } = await import(
      "../../src/pipeline/producers/candidateRecordRolloverProducer.js"
    );

    const result = await runCandidateRecordRolloverProducer();

    expect(result.enabled).toBe(false);
    expect(result.dueRows).toBe(0);
    expect(result.selectedRows).toBe(0);
    expect(result.emittedRows).toBe(0);
  });

  it("enqueues due candidates and reports cap hit", async () => {
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

    vi.doMock("../../src/pipeline/candidates/candidateRecordDraftEmitter.js", () => ({
      enqueueCandidateRecordDrafts: enqueueDraftsMock,
    }));

    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          candidate_id: "cand-1",
          election_id: "e-1",
          total_due_rows: "3",
        },
        {
          candidate_id: "cand-2",
          election_id: "e-2",
          total_due_rows: "3",
        },
      ],
    });

    enqueueDraftsMock.mockResolvedValueOnce({ emittedCount: 1, skippedCount: 1 });

    const { runCandidateRecordRolloverProducer } = await import(
      "../../src/pipeline/producers/candidateRecordRolloverProducer.js"
    );

    const result = await runCandidateRecordRolloverProducer();

    expect(poolQueryMock).toHaveBeenCalledTimes(1);
    expect(enqueueDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      { candidateId: "cand-1", electionId: "e-1", runId: expect.any(String) },
      { candidateId: "cand-2", electionId: "e-2", runId: expect.any(String) },
    ]);

    expect(result).toMatchObject({
      enabled: true,
      forced: false,
      cooldownDays: 30,
      maxEnqueuePerRun: 2,
      dueRows: 3,
      selectedRows: 2,
      maxEnqueueHit: true,
      emittedRows: 1,
      markerSkippedRows: 1,
    });
  });

  it("runs when forced even if feature flag is off", async () => {
    process.env.CANDIDATE_RECORD_ENABLE_DAILY_ROLLOVER_PRODUCER = "false";

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

    vi.doMock("../../src/pipeline/candidates/candidateRecordDraftEmitter.js", () => ({
      enqueueCandidateRecordDrafts: enqueueDraftsMock,
    }));

    poolQueryMock.mockResolvedValueOnce({ rows: [] });
    enqueueDraftsMock.mockResolvedValueOnce({ emittedCount: 0, skippedCount: 0 });

    const { runCandidateRecordRolloverProducer } = await import(
      "../../src/pipeline/producers/candidateRecordRolloverProducer.js"
    );

    const result = await runCandidateRecordRolloverProducer({ force: true });

    expect(result.enabled).toBe(true);
    expect(result.forced).toBe(true);
    expect(poolQueryMock).toHaveBeenCalledTimes(1);
  });
});
