import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("georgiaCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  function mockEnv(redisUrl = "redis://localhost:6379/0") {
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: redisUrl,
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));
  }

  it("returns a disabled no-op result when the sync flag is off", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-01T00:00:00.000Z"));
    process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runGeorgiaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/georgiaCandidateFinanceSyncScheduler.js"
    );

    const result = await runGeorgiaCandidateFinanceSyncJob({
      triggeredBy: "daily",
      maxCandidates: 10,
      staleAfterDays: 7,
    });

    expect(result).toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      dryRun: false,
      now: "2026-08-01T00:00:00.000Z",
      staleAfterDays: 7,
      maxCandidates: 10,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      independentExpenditureStoreError: null,
      results: [],
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("rejects unsafe integer job options that Number() rounding could smuggle in", async () => {
    process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runGeorgiaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/georgiaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      runGeorgiaCandidateFinanceSyncJob({ triggeredBy: "manual", maxCandidates: 2 ** 53 })
    ).rejects.toThrow("Invalid Georgia finance sync scheduler maxCandidates");
    expect(Pool).not.toHaveBeenCalled();
  });

  it("does not let force bypass the master Georgia finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-01T00:00:00.000Z"));
    process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runGeorgiaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/georgiaCandidateFinanceSyncScheduler.js"
    );

    const result = await runGeorgiaCandidateFinanceSyncJob({
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

  it("runs the due Georgia finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-01T00:00:00.000Z"));
    process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueGeorgiaCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: true,
      now: "2026-08-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 4,
      selectedCandidateCount: 2,
      syncedCandidateCount: 2,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      independentExpenditureStoreError: null,
      results: [],
    });

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    mockEnv();
    vi.doMock("../../src/pipeline/georgiaFinance/georgiaCandidateFinanceBatchSync.js", () => ({
      syncDueGeorgiaCandidateFinance,
    }));

    const { runGeorgiaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/georgiaCandidateFinanceSyncScheduler.js"
    );

    const result = await runGeorgiaCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      selectedCandidateCount: 2,
    });
    expect(syncDueGeorgiaCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "55 10 * * *";
    process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/New_York";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringGeorgiaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/georgiaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringGeorgiaCandidateFinanceSyncJobs({
      maxCandidates: 5,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "georgia_candidate_finance_sync_daily",
      {
        pattern: "55 10 * * *",
        tz: "America/New_York",
      },
      expect.objectContaining({
        name: "georgia_candidate_finance_sync_due",
        data: expect.objectContaining({
          maxCandidates: 5,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes recurring jobs when the master Georgia finance flag is disabled", async () => {
    process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringGeorgiaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/georgiaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringGeorgiaCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("georgia_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("enqueues manual jobs with requested options", async () => {
    process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "georgia-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualGeorgiaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/georgiaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualGeorgiaCandidateFinanceSyncJob({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
      })
    ).resolves.toBe("georgia-finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "georgia_candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects manual job IDs that collide with scheduler sentinel return values", async () => {
    process.env.GEORGIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "disabled" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualGeorgiaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/georgiaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualGeorgiaCandidateFinanceSyncJob({ force: true }, { jobId: "disabled" })
    ).rejects.toThrow("Georgia finance sync scheduler jobId uses a reserved value");
    await expect(
      enqueueManualGeorgiaCandidateFinanceSyncJob({ force: true }, { jobId: "unknown" })
    ).rejects.toThrow("Georgia finance sync scheduler jobId uses a reserved value");

    expect(queueInstance.add).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(2);
  });
});
