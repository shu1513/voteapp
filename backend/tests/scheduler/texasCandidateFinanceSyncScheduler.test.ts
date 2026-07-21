import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("texasCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("returns a disabled no-op result when the sync flag is off and not forced", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runTexasCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/texasCandidateFinanceSyncScheduler.js"
    );

    const result = await runTexasCandidateFinanceSyncJob({
      triggeredBy: "daily",
      maxCandidates: 10,
      staleAfterDays: 7,
    });

    expect(result).toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      dryRun: false,
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

  it("does not let force bypass the master Texas finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runTexasCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/texasCandidateFinanceSyncScheduler.js"
    );

    const result = await runTexasCandidateFinanceSyncJob({
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

  it("runs the due Texas finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueTexasCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: true,
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
    vi.doMock("../../src/pipeline/texasFinance/texasCandidateFinanceBatchSync.js", () => ({
      syncDueTexasCandidateFinance,
    }));

    const { runTexasCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/texasCandidateFinanceSyncScheduler.js"
    );

    const result = await runTexasCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      rawDataZipPath: "/tmp/2026_ContributionData.csv.zip",
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      selectedCandidateCount: 2,
    });
    expect(syncDueTexasCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        rawDataZipPath: "/tmp/2026_ContributionData.csv.zip",
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "15 9 * * *";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/Denver";

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

    const { upsertRecurringTexasCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/texasCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringTexasCandidateFinanceSyncJobs({
      maxCandidates: 5,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "texas_candidate_finance_sync_daily",
      {
        pattern: "15 9 * * *",
        tz: "America/Denver",
      },
      expect.objectContaining({
        name: "texas_candidate_finance_sync_due",
        data: expect.objectContaining({
          maxCandidates: 5,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects malformed Redis DB path segments", async () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";

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

    const { upsertRecurringTexasCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/texasCandidateFinanceSyncScheduler.js"
    );

    await expect(upsertRecurringTexasCandidateFinanceSyncJobs()).rejects.toThrow("Invalid REDIS_URL db index");
    expect(Queue).not.toHaveBeenCalled();
  });

  it("removes the recurring scheduler when the master Texas finance flag is disabled", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "false";

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

    const { upsertRecurringTexasCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/texasCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringTexasCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("texas_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("does not enqueue manual jobs when the master Texas finance flag is disabled", async () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "texas-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualTexasCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/texasCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualTexasCandidateFinanceSyncJob({ force: true })).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
    expect(queueInstance.add).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs with requested options", async () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "texas-finance-job-1" }),
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

    const { enqueueManualTexasCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/texasCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualTexasCandidateFinanceSyncJob({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        rawDataCacheDir: "/tmp/texas-cache",
      })
    ).resolves.toBe("texas-finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "texas_candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        rawDataCacheDir: "/tmp/texas-cache",
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
