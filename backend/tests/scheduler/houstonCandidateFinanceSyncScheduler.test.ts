import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("houstonCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("returns a disabled no-op result when the sync flag is off and not forced", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runHoustonCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    const result = await runHoustonCandidateFinanceSyncJob({
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
      outsideSourceAvailable: false,
      results: [],
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("does not let force bypass the master Houston finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runHoustonCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    const result = await runHoustonCandidateFinanceSyncJob({
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

  it("runs the due Houston finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueHoustonCandidateFinance = vi.fn().mockResolvedValue({
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
    vi.doMock("../../src/pipeline/houstonFinance/houstonCandidateFinanceBatchSync.js", () => ({
      syncDueHoustonCandidateFinance,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv: vi.fn(() => vi.fn()),
    }));

    const { runHoustonCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    const result = await runHoustonCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      rawDataZipPath: "/tmp/2026_ContributionData.csv.zip",
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
    expect(syncDueHoustonCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        rawDataZipPath: "/tmp/2026_ContributionData.csv.zip",
        financeIndustryClassifier: undefined,
        aiClassificationMinAmount: 25000,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("passes the shared finance industry classifier when AI classification is enabled outside dry-run", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueHoustonCandidateFinance = vi.fn().mockResolvedValue({
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
    vi.doMock("../../src/pipeline/houstonFinance/houstonCandidateFinanceBatchSync.js", () => ({
      syncDueHoustonCandidateFinance,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv,
    }));

    const { runHoustonCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    const result = await runHoustonCandidateFinanceSyncJob({
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
    expect(syncDueHoustonCandidateFinance).toHaveBeenCalledWith(
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
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "15 9 * * *";
    process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/Denver";

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

    const { upsertRecurringHoustonCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringHoustonCandidateFinanceSyncJobs({
      maxCandidates: 5,
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "houston_candidate_finance_sync_daily",
      {
        pattern: "15 9 * * *",
        tz: "America/Denver",
      },
      expect.objectContaining({
        name: "houston_candidate_finance_sync_due",
        data: expect.objectContaining({
          maxCandidates: 5,
          aiClassifyIndustries: true,
          aiClassificationMinAmount: 25000,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects malformed Redis DB path segments", async () => {
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "true";

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

    const { upsertRecurringHoustonCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    await expect(upsertRecurringHoustonCandidateFinanceSyncJobs()).rejects.toThrow("Invalid REDIS_URL db index");
    expect(Queue).not.toHaveBeenCalled();
  });

  it("removes the recurring scheduler when the master Houston finance flag is disabled", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "false";

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

    const { upsertRecurringHoustonCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringHoustonCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("houston_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("does not enqueue manual jobs when the master Houston finance flag is disabled", async () => {
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "houston-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualHoustonCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualHoustonCandidateFinanceSyncJob({ force: true })).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
    expect(queueInstance.add).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs with requested options", async () => {
    process.env.HOUSTON_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "houston-finance-job-1" }),
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

    const { enqueueManualHoustonCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/houstonCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualHoustonCandidateFinanceSyncJob({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        rawDataCacheDir: "/tmp/houston-cache",
        aiClassifyIndustries: true,
        aiClassificationMinAmount: 25000,
      })
    ).resolves.toBe("houston-finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "houston_candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        rawDataCacheDir: "/tmp/houston-cache",
        aiClassifyIndustries: true,
        aiClassificationMinAmount: 25000,
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
