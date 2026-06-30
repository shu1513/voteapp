import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("illinoisCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED;
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

  it("returns a disabled no-op result when the sync flag is off and not forced", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runIllinoisCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    const result = await runIllinoisCandidateFinanceSyncJob({
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

  it("does not let force bypass the master Illinois finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runIllinoisCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    const result = await runIllinoisCandidateFinanceSyncJob({
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

  it("runs the due Illinois finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), connect: vi.fn(), end };
    const syncDueIllinoisCandidateFinance = vi.fn().mockResolvedValue({
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
    vi.doMock("../../src/pipeline/illinoisFinance/illinoisCandidateFinanceBatchSync.js", () => ({
      syncDueIllinoisCandidateFinance,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv: vi.fn(() => vi.fn()),
    }));

    const { runIllinoisCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    const result = await runIllinoisCandidateFinanceSyncJob({
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
    expect(syncDueIllinoisCandidateFinance).toHaveBeenCalledWith(
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
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), connect: vi.fn(), end };
    const syncDueIllinoisCandidateFinance = vi.fn().mockResolvedValue({
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
    vi.doMock("../../src/pipeline/illinoisFinance/illinoisCandidateFinanceBatchSync.js", () => ({
      syncDueIllinoisCandidateFinance,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv,
    }));

    const { runIllinoisCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    const result = await runIllinoisCandidateFinanceSyncJob({
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
    expect(syncDueIllinoisCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: false,
        financeIndustryClassifier: classifier,
        aiClassificationMinAmount: 25000,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("loads artifact data and passes artifact-backed resolvers to due sync jobs", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), connect: vi.fn(), end };
    const syncDueIllinoisCandidateFinance = vi.fn().mockResolvedValue({
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
    const artifacts = {
      contributionRecords: [],
      contributionSourceUrl: "https://example.test/contributions.csv",
    };
    const resolveCandidateCommittee = vi.fn();
    const loadIllinoisSbeArtifactDataSet = vi.fn().mockResolvedValue(artifacts);
    const createIllinoisSbeArtifactCandidateCommitteeResolver = vi.fn(() => resolveCandidateCommittee);
    const loadIllinoisFinanceDataForDueRowFromArtifacts = vi.fn();

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    mockEnv();
    vi.doMock("../../src/pipeline/illinoisFinance/illinoisCandidateFinanceBatchSync.js", () => ({
      syncDueIllinoisCandidateFinance,
    }));
    vi.doMock("../../src/pipeline/illinoisFinance/illinoisSbeArtifactDataSource.js", () => ({
      loadIllinoisSbeArtifactDataSet,
      createIllinoisSbeArtifactCandidateCommitteeResolver,
      loadIllinoisFinanceDataForDueRowFromArtifacts,
    }));
    vi.doMock("../../src/ai/classifyFinanceIndustry.js", () => ({
      createFinanceIndustryClassifierFromEnv: vi.fn(() => vi.fn()),
    }));

    const { runIllinoisCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    const result = await runIllinoisCandidateFinanceSyncJob({
      maxCandidates: 1,
      contributionCsvPaths: ["/exports/il-contrib.csv"],
      expenditureCsvPaths: ["/exports/il-exp.csv"],
      contributionSourceUrl: "https://example.test/contributions.csv",
      expenditureSourceUrl: "https://example.test/expenditures.csv",
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      selectedCandidateCount: 1,
      autoLinkLinkedCount: 1,
    });
    expect(loadIllinoisSbeArtifactDataSet).toHaveBeenCalledWith({
      contributionCsvPaths: ["/exports/il-contrib.csv"],
      expenditureCsvPaths: ["/exports/il-exp.csv"],
      contributionSourceUrl: "https://example.test/contributions.csv",
      expenditureSourceUrl: "https://example.test/expenditures.csv",
    });
    expect(createIllinoisSbeArtifactCandidateCommitteeResolver).toHaveBeenCalledWith(artifacts);
    expect(syncDueIllinoisCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        resolveCandidateCommittee,
        loadIllinoisFinanceDataFn: expect.any(Function),
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "5 9 * * *";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/Chicago";

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    mockEnv();

    const { upsertRecurringIllinoisCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringIllinoisCandidateFinanceSyncJobs({
      maxCandidates: 5,
      aiClassifyIndustries: true,
      contributionCsvPaths: [" /exports/il-contrib.csv "],
      expenditureCsvPaths: [" /exports/il-exp.csv "],
      contributionSourceUrl: " https://example.test/contributions.csv ",
      expenditureSourceUrl: " https://example.test/expenditures.csv ",
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "illinois_candidate_finance_sync_daily",
      {
        pattern: "5 9 * * *",
        tz: "America/Chicago",
      },
      expect.objectContaining({
        name: "illinois_candidate_finance_sync_due",
        data: expect.objectContaining({
          maxCandidates: 5,
          aiClassifyIndustries: true,
          contributionCsvPaths: ["/exports/il-contrib.csv"],
          expenditureCsvPaths: ["/exports/il-exp.csv"],
          contributionSourceUrl: "https://example.test/contributions.csv",
          expenditureSourceUrl: "https://example.test/expenditures.csv",
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects malformed Redis DB path segments before creating a queue", async () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";

    const Queue = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    mockEnv("redis://localhost:6379/0/foo");

    const { upsertRecurringIllinoisCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    await expect(upsertRecurringIllinoisCandidateFinanceSyncJobs()).rejects.toThrow("Invalid REDIS_URL db index");
    expect(Queue).not.toHaveBeenCalled();
  });

  it("removes the recurring scheduler when the master Illinois finance flag is disabled", async () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "false";

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    mockEnv();

    const { upsertRecurringIllinoisCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringIllinoisCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("illinois_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring scheduler when Illinois finance sync is disabled", async () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    mockEnv();

    const { upsertRecurringIllinoisCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringIllinoisCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("illinois_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("does not enqueue manual jobs when the master Illinois finance flag is disabled", async () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "illinois-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualIllinoisCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualIllinoisCandidateFinanceSyncJob({ force: true })).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
    expect(queueInstance.add).not.toHaveBeenCalled();
  });

  it("does not enqueue manual jobs when Illinois finance sync is disabled and not forced", async () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "illinois-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualIllinoisCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualIllinoisCandidateFinanceSyncJob()).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
    expect(queueInstance.add).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs with requested options", async () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "illinois-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    mockEnv();

    const { enqueueManualIllinoisCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualIllinoisCandidateFinanceSyncJob({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        aiClassifyIndustries: true,
        contributionCsvPaths: [" /exports/il-contrib.csv "],
        expenditureCsvPaths: [" /exports/il-exp.csv "],
        contributionSourceUrl: " https://example.test/contributions.csv ",
        expenditureSourceUrl: " https://example.test/expenditures.csv ",
      })
    ).resolves.toBe("illinois-finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "illinois_candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        aiClassifyIndustries: true,
        contributionCsvPaths: ["/exports/il-contrib.csv"],
        expenditureCsvPaths: ["/exports/il-exp.csv"],
        contributionSourceUrl: "https://example.test/contributions.csv",
        expenditureSourceUrl: "https://example.test/expenditures.csv",
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("dedupes manual jobs when an explicit job id is provided", async () => {
    process.env.ILLINOIS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "illinois-candidate-finance-linked-election-sync-2026-06-01" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    mockEnv();

    const {
      buildIllinoisCandidateFinanceLinkedElectionSyncJobId,
      enqueueManualIllinoisCandidateFinanceSyncJob,
    } = await import("../../src/scheduler/illinoisCandidateFinanceSyncScheduler.js");

    const jobId = buildIllinoisCandidateFinanceLinkedElectionSyncJobId(
      new Date("2026-06-01T12:00:00.000Z")
    );

    await expect(
      enqueueManualIllinoisCandidateFinanceSyncJob(
        {
          force: true,
        },
        { jobId }
      )
    ).resolves.toBe("illinois-candidate-finance-linked-election-sync-2026-06-01");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "illinois_candidate_finance_sync_due",
      expect.objectContaining({
        force: true,
        triggeredBy: "manual",
      }),
      expect.objectContaining({
        jobId: "illinois-candidate-finance-linked-election-sync-2026-06-01",
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
