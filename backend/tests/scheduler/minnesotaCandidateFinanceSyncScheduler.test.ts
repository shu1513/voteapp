import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("minnesotaCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.MINNESOTA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.MINNESOTA_CAMPAIGN_FINANCE_SYNC_ENABLED;
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
    process.env.MINNESOTA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MINNESOTA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));
    mockEnv();

    const { runMinnesotaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/minnesotaCandidateFinanceSyncScheduler.js"
    );

    const result = await runMinnesotaCandidateFinanceSyncJob({
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

  it("runs the due Minnesota finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.MINNESOTA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MINNESOTA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueMinnesotaCandidateFinance = vi.fn().mockResolvedValue({
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
    vi.doMock("../../src/pipeline/minnesotaFinance/minnesotaCandidateFinanceBatchSync.js", () => ({
      syncDueMinnesotaCandidateFinance,
    }));

    const { runMinnesotaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/minnesotaCandidateFinanceSyncScheduler.js"
    );

    const result = await runMinnesotaCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      rawDataCacheDir: "/tmp/mn",
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      selectedCandidateCount: 2,
    });
    expect(syncDueMinnesotaCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        rawDataCacheDir: "/tmp/mn",
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring scheduler when the master Minnesota finance flag is disabled", async () => {
    process.env.MINNESOTA_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMinnesotaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/minnesotaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringMinnesotaCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("minnesota_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("upserts the recurring scheduler when the master Minnesota finance flag is enabled", async () => {
    process.env.MINNESOTA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MINNESOTA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMinnesotaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/minnesotaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringMinnesotaCandidateFinanceSyncJobs({
      rawDataCacheDir: "/tmp/mn",
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "minnesota_candidate_finance_sync_daily",
      expect.objectContaining({
        pattern: "45 9 * * *",
        tz: "UTC",
      }),
      expect.objectContaining({
        name: "minnesota_candidate_finance_sync_due",
        data: expect.objectContaining({
          rawDataCacheDir: "/tmp/mn",
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.removeJobScheduler).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs with a deterministic linked-election job id", async () => {
    process.env.MINNESOTA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MINNESOTA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "minnesota-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const {
      buildMinnesotaCandidateFinanceLinkedElectionSyncJobId,
      enqueueManualMinnesotaCandidateFinanceSyncJob,
    } = await import("../../src/scheduler/minnesotaCandidateFinanceSyncScheduler.js");

    const jobId = buildMinnesotaCandidateFinanceLinkedElectionSyncJobId(
      new Date("2026-11-03T23:00:00.000Z")
    );
    await expect(
      enqueueManualMinnesotaCandidateFinanceSyncJob({ rawDataCacheDir: "/tmp/mn" }, { jobId })
    ).resolves.toBe("minnesota-finance-job-1");

    expect(jobId).toBe("minnesota-candidate-finance-linked-election-sync-2026-11-03");
    expect(queueInstance.add).toHaveBeenCalledWith(
      "minnesota_candidate_finance_sync_due",
      expect.objectContaining({
        rawDataCacheDir: "/tmp/mn",
        triggeredBy: "manual",
      }),
      expect.objectContaining({ jobId })
    );
  });
});
