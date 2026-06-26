import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("maineCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.MAINE_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED;
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
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runMaineCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/maineCandidateFinanceSyncScheduler.js"
    );

    const result = await runMaineCandidateFinanceSyncJob({
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

  it("does not let force bypass the master Maine finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runMaineCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/maineCandidateFinanceSyncScheduler.js"
    );

    const result = await runMaineCandidateFinanceSyncJob({
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

  it("runs the due Maine finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueMaineCandidateFinance = vi.fn().mockResolvedValue({
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
    mockEnv();
    vi.doMock("../../src/pipeline/maineFinance/maineCandidateFinanceBatchSync.js", () => ({
      syncDueMaineCandidateFinance,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv: vi.fn(() => vi.fn()),
    }));

    const { runMaineCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/maineCandidateFinanceSyncScheduler.js"
    );

    const result = await runMaineCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      rawDataCacheDir: "/tmp/maine-cfis",
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      selectedCandidateCount: 2,
    });
    expect(syncDueMaineCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        rawDataCacheDir: "/tmp/maine-cfis",
        financeIndustryClassifier: undefined,
        aiClassificationMinAmount: 25000,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("passes the shared finance industry classifier when AI classification is enabled outside dry-run", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueMaineCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: false,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 7,
      maxCandidates: 1,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
      results: [],
    });
    const classifier = vi.fn();
    const createFinanceIndustryClassifierFromEnv = vi.fn(() => classifier);

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    mockEnv();
    vi.doMock("../../src/pipeline/maineFinance/maineCandidateFinanceBatchSync.js", () => ({
      syncDueMaineCandidateFinance,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv,
    }));

    const { runMaineCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/maineCandidateFinanceSyncScheduler.js"
    );

    const result = await runMaineCandidateFinanceSyncJob({
      maxCandidates: 1,
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      dryRun: false,
      selectedCandidateCount: 1,
    });
    expect(createFinanceIndustryClassifierFromEnv).toHaveBeenCalledTimes(1);
    expect(syncDueMaineCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: false,
        maxCandidates: 1,
        financeIndustryClassifier: classifier,
        aiClassificationMinAmount: 25000,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "55 9 * * *";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/New_York";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMaineCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/maineCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringMaineCandidateFinanceSyncJobs({
      maxCandidates: 5,
      rawDataCacheDir: "/tmp/maine-cfis",
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "maine_candidate_finance_sync_daily",
      {
        pattern: "55 9 * * *",
        tz: "America/New_York",
      },
      expect.objectContaining({
        name: "maine_candidate_finance_sync_due",
        data: expect.objectContaining({
          maxCandidates: 5,
          rawDataCacheDir: "/tmp/maine-cfis",
          aiClassifyIndustries: true,
          aiClassificationMinAmount: 25000,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes recurring jobs when the master Maine finance flag is disabled", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMaineCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/maineCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringMaineCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("maine_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("enqueues manual jobs with requested options", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "maine-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualMaineCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/maineCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualMaineCandidateFinanceSyncJob({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        rawDataCacheDir: "/tmp/maine-cfis",
        aiClassifyIndustries: true,
        aiClassificationMinAmount: 25000,
      })
    ).resolves.toBe("maine-finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "maine_candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        rawDataCacheDir: "/tmp/maine-cfis",
        aiClassifyIndustries: true,
        aiClassificationMinAmount: 25000,
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects manual job IDs that collide with scheduler sentinel return values", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "disabled" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualMaineCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/maineCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualMaineCandidateFinanceSyncJob({ force: true }, { jobId: "disabled" })).rejects.toThrow(
      "Maine finance sync scheduler jobId uses a reserved value"
    );
    await expect(enqueueManualMaineCandidateFinanceSyncJob({ force: true }, { jobId: "unknown" })).rejects.toThrow(
      "Maine finance sync scheduler jobId uses a reserved value"
    );

    expect(queueInstance.add).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(2);
  });
});
