import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("californiaCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("returns a disabled no-op result when the sync flag is off and not forced", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runCaliforniaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    const result = await runCaliforniaCandidateFinanceSyncJob({
      triggeredBy: "daily",
      maxCandidates: 10,
      staleAfterDays: 7,
    });

    expect(result).toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      dryRun: false,
      includeOutside: true,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 7,
      maxCandidates: 10,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      failedCandidateCount: 0,
      results: [],
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("does not let force bypass the master California finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runCaliforniaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    const result = await runCaliforniaCandidateFinanceSyncJob({
      triggeredBy: "manual",
      force: true,
      maxCandidates: 10,
      staleAfterDays: 7,
    });

    expect(result).toMatchObject({
      enabled: false,
      force: true,
      triggeredBy: "manual",
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("runs the due California finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueCaliforniaCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: true,
      includeOutside: false,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 4,
      selectedCandidateCount: 2,
      syncedCandidateCount: 2,
      failedCandidateCount: 0,
      results: [],
    });

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
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
    vi.doMock("../../src/pipeline/californiaFinance/californiaCandidateFinanceBatchSync.js", () => ({
      syncDueCaliforniaCandidateFinance,
    }));

    const { runCaliforniaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    const result = await runCaliforniaCandidateFinanceSyncJob({
      dryRun: true,
      includeOutside: false,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      timeoutMs: 5000,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      includeOutside: false,
      selectedCandidateCount: 2,
    });
    expect(syncDueCaliforniaCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        includeOutside: false,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        powerSearchOptions: { timeoutMs: 5000 },
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "5 9 * * *";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/Los_Angeles";

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
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

    const { upsertRecurringCaliforniaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringCaliforniaCandidateFinanceSyncJobs({
      maxCandidates: 5,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "california_candidate_finance_sync_daily",
      {
        pattern: "5 9 * * *",
        tz: "America/Los_Angeles",
      },
      expect.objectContaining({
        name: "california_candidate_finance_sync_due",
        data: expect.objectContaining({
          includeOutside: true,
          maxCandidates: 5,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects malformed Redis DB path segments", async () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";

    const Queue = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0/foo",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));

    const { upsertRecurringCaliforniaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    await expect(upsertRecurringCaliforniaCandidateFinanceSyncJobs()).rejects.toThrow("Invalid REDIS_URL db index");
    expect(Queue).not.toHaveBeenCalled();
  });

  it("removes the recurring scheduler when the master California finance flag is disabled", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "false";

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
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

    const { upsertRecurringCaliforniaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringCaliforniaCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("california_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("does not enqueue manual jobs when the master California finance flag is disabled", async () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "california-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualCaliforniaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualCaliforniaCandidateFinanceSyncJob({ force: true })).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
    expect(queueInstance.add).not.toHaveBeenCalled();
  });

  it("does not enqueue manual jobs when California finance sync is disabled and not forced", async () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "california-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualCaliforniaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualCaliforniaCandidateFinanceSyncJob()).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
    expect(queueInstance.add).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs with requested options", async () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "california-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
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

    const { enqueueManualCaliforniaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/californiaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualCaliforniaCandidateFinanceSyncJob({
        dryRun: true,
        force: true,
        includeOutside: false,
        maxCandidates: 3,
        electionLookbackDays: 21,
      })
    ).resolves.toBe("california-finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "california_candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: true,
        force: true,
        includeOutside: false,
        maxCandidates: 3,
        electionLookbackDays: 21,
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
