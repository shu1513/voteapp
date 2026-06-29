import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("nebraskaCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.NEBRASKA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  function mockEnv(redisUrl = "redis://localhost:6379/4") {
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgres://test/nebraska",
        REDIS_URL: redisUrl,
      }),
    }));
  }

  it("returns a disabled no-op result when the sync flag is off and not forced", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.NEBRASKA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));
    mockEnv();

    const { runNebraskaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/nebraskaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      runNebraskaCandidateFinanceSyncJob({
        triggeredBy: "daily",
        maxCandidates: 10,
        staleAfterDays: 7,
      })
    ).resolves.toEqual({
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

  it("runs the due Nebraska finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.NEBRASKA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueNebraskaCandidateFinance = vi.fn().mockResolvedValue({
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
    vi.doMock("../../src/pipeline/nebraskaFinance/nebraskaCandidateFinanceBatchSync.js", () => ({
      syncDueNebraskaCandidateFinance,
    }));

    const { runNebraskaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/nebraskaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      runNebraskaCandidateFinanceSyncJob({
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        rawDataCacheDir: "/tmp/ne",
        rawDataZipPath: "/tmp/ne.zip",
        triggeredBy: "manual",
      })
    ).resolves.toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      selectedCandidateCount: 2,
    });
    expect(syncDueNebraskaCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        rawDataCacheDir: "/tmp/ne",
        rawDataZipPath: "/tmp/ne.zip",
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts and removes the recurring BullMQ scheduler", async () => {
    process.env.NEBRASKA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "15 10 * * *";
    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/Chicago";
    mockEnv();

    const upsertJobScheduler = vi.fn().mockResolvedValue(undefined);
    const removeJobScheduler = vi.fn().mockResolvedValue(undefined);
    const close = vi.fn().mockResolvedValue(undefined);
    const Queue = vi.fn(() => ({ upsertJobScheduler, removeJobScheduler, close }));
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringNebraskaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/nebraskaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringNebraskaCandidateFinanceSyncJobs({
      dryRun: true,
      maxCandidates: 2,
      rawDataCacheDir: "/tmp/ne",
      rawDataZipPath: "/tmp/ne.zip",
    });

    expect(upsertJobScheduler).toHaveBeenCalledWith(
      "nebraska_candidate_finance_sync_daily",
      {
        pattern: "15 10 * * *",
        tz: "America/Chicago",
      },
      {
        name: "nebraska_candidate_finance_sync_due",
        data: expect.objectContaining({
          dryRun: true,
          force: false,
          maxCandidates: 2,
          rawDataCacheDir: "/tmp/ne",
          rawDataZipPath: "/tmp/ne.zip",
          triggeredBy: "daily",
        }),
        opts: expect.objectContaining({
          removeOnComplete: 1000,
          removeOnFail: 1000,
        }),
      }
    );

    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    await upsertRecurringNebraskaCandidateFinanceSyncJobs();
    expect(removeJobScheduler).toHaveBeenCalledWith("nebraska_candidate_finance_sync_daily");

    process.env.NEBRASKA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    await upsertRecurringNebraskaCandidateFinanceSyncJobs();
    expect(removeJobScheduler).toHaveBeenCalledTimes(2);
    expect(close).toHaveBeenCalledTimes(3);
  });

  it("does not let force persist a recurring scheduler bypass", async () => {
    process.env.NEBRASKA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    mockEnv();

    const upsertJobScheduler = vi.fn().mockResolvedValue(undefined);
    const removeJobScheduler = vi.fn().mockResolvedValue(undefined);
    const close = vi.fn().mockResolvedValue(undefined);
    const Queue = vi.fn(() => ({ upsertJobScheduler, removeJobScheduler, close }));
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringNebraskaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/nebraskaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringNebraskaCandidateFinanceSyncJobs({ force: true });

    expect(removeJobScheduler).toHaveBeenCalledWith("nebraska_candidate_finance_sync_daily");
    expect(upsertJobScheduler).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledTimes(1);

    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    await upsertRecurringNebraskaCandidateFinanceSyncJobs({ force: true });

    expect(upsertJobScheduler).toHaveBeenCalledWith(
      "nebraska_candidate_finance_sync_daily",
      expect.any(Object),
      expect.objectContaining({
        data: expect.objectContaining({
          force: false,
          triggeredBy: "daily",
        }),
      })
    );
  });

  it("enqueues manual jobs with a deterministic linked-election job id", async () => {
    process.env.NEBRASKA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "nebraska-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    vi.doMock("bullmq", () => ({ Queue: vi.fn(() => queueInstance), Worker: vi.fn() }));

    const {
      buildNebraskaCandidateFinanceLinkedElectionSyncJobId,
      enqueueManualNebraskaCandidateFinanceSyncJob,
    } = await import("../../src/scheduler/nebraskaCandidateFinanceSyncScheduler.js");

    const jobId = buildNebraskaCandidateFinanceLinkedElectionSyncJobId(
      new Date("2026-11-03T23:00:00.000Z")
    );
    await expect(
      enqueueManualNebraskaCandidateFinanceSyncJob(
        { rawDataCacheDir: "/tmp/ne", rawDataZipPath: "/tmp/ne.zip" },
        { jobId }
      )
    ).resolves.toBe("nebraska-finance-job-1");

    expect(jobId).toBe("nebraska-candidate-finance-linked-election-sync-2026-11-03");
    expect(queueInstance.add).toHaveBeenCalledWith(
      "nebraska_candidate_finance_sync_due",
      expect.objectContaining({
        rawDataCacheDir: "/tmp/ne",
        rawDataZipPath: "/tmp/ne.zip",
        triggeredBy: "manual",
      }),
      expect.objectContaining({ jobId })
    );
  });
});
