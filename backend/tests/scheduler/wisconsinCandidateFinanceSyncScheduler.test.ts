import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("wisconsinCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.WISCONSIN_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.WISCONSIN_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  function mockEnv(redisUrl = "redis://localhost:6379/0") {
    vi.doMock("../../src/config/env.js", () => ({
      loadProjectEnv: vi.fn(),
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

  it("returns a disabled no-op result when the sync flag is off and not forced", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.WISCONSIN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.WISCONSIN_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runWisconsinCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/wisconsinCandidateFinanceSyncScheduler.js"
    );

    const result = await runWisconsinCandidateFinanceSyncJob({
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
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      results: [],
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("does not let force bypass the master Wisconsin finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.WISCONSIN_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.WISCONSIN_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runWisconsinCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/wisconsinCandidateFinanceSyncScheduler.js"
    );

    const result = await runWisconsinCandidateFinanceSyncJob({
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

  it("runs the due Wisconsin finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.WISCONSIN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.WISCONSIN_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueWisconsinCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: true,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 4,
      selectedCandidateCount: 2,
      syncedCandidateCount: 2,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      results: [],
    });

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    mockEnv();
    vi.doMock("../../src/pipeline/wisconsinFinance/wisconsinCandidateFinanceBatchSync.js", () => ({
      syncDueWisconsinCandidateFinance,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv: vi.fn(() => vi.fn()),
    }));

    const { runWisconsinCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/wisconsinCandidateFinanceSyncScheduler.js"
    );

    const result = await runWisconsinCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
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
    expect(syncDueWisconsinCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        financeIndustryClassifier: undefined,
        aiClassificationMinAmount: 25000,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("passes the shared finance industry classifier when AI classification is enabled outside dry-run", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.WISCONSIN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.WISCONSIN_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueWisconsinCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: false,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 7,
      maxCandidates: 1,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
      results: [],
    });
    const classifier = vi.fn();
    const createFinanceIndustryClassifierFromEnv = vi.fn(() => classifier);

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    mockEnv();
    vi.doMock("../../src/pipeline/wisconsinFinance/wisconsinCandidateFinanceBatchSync.js", () => ({
      syncDueWisconsinCandidateFinance,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv,
    }));

    const { runWisconsinCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/wisconsinCandidateFinanceSyncScheduler.js"
    );

    const result = await runWisconsinCandidateFinanceSyncJob({
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
    expect(syncDueWisconsinCandidateFinance).toHaveBeenCalledWith(
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
    process.env.WISCONSIN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.WISCONSIN_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "15 9 * * *";
    process.env.WISCONSIN_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/Los_Angeles";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringWisconsinCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/wisconsinCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringWisconsinCandidateFinanceSyncJobs({
      maxCandidates: 5,
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "wisconsin_candidate_finance_sync_daily",
      {
        pattern: "15 9 * * *",
        tz: "America/Los_Angeles",
      },
      expect.objectContaining({
        name: "wisconsin_candidate_finance_sync_due",
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

  it("removes the recurring scheduler when the master Wisconsin finance flag is disabled", async () => {
    process.env.WISCONSIN_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringWisconsinCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/wisconsinCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringWisconsinCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("wisconsin_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs with a deterministic linked-election job id", async () => {
    process.env.WISCONSIN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.WISCONSIN_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "wisconsin-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const {
      buildWisconsinCandidateFinanceLinkedElectionSyncJobId,
      enqueueManualWisconsinCandidateFinanceSyncJob,
    } = await import("../../src/scheduler/wisconsinCandidateFinanceSyncScheduler.js");

    const jobId = buildWisconsinCandidateFinanceLinkedElectionSyncJobId(
      new Date("2026-11-03T23:00:00.000Z")
    );
    await expect(
      enqueueManualWisconsinCandidateFinanceSyncJob(
        { aiClassifyIndustries: true, aiClassificationMinAmount: 25000 },
        { jobId }
      )
    ).resolves.toBe("wisconsin-finance-job-1");

    expect(jobId).toBe("wisconsin-candidate-finance-linked-election-sync-2026-11-03");
    expect(queueInstance.add).toHaveBeenCalledWith(
      "wisconsin_candidate_finance_sync_due",
      expect.objectContaining({
        aiClassifyIndustries: true,
        aiClassificationMinAmount: 25000,
        triggeredBy: "manual",
      }),
      expect.objectContaining({ jobId })
    );
  });
});
