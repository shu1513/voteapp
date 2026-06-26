import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("newJerseyCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_ENABLED;
    delete process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_DAILY_CRON;
    delete process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_DAILY_TZ;
    delete process.env.DATABASE_URL;
    delete process.env.REDIS_URL;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  function mockEnv(redisUrl = "redis://localhost:6379/0") {
    process.env.DATABASE_URL = "postgresql://localhost:5432/test";
    process.env.REDIS_URL = redisUrl;
  }

  it("returns a disabled no-op result when the sync flag is off and not forced", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runNewJerseyCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js"
    );

    const result = await runNewJerseyCandidateFinanceSyncJob({
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
      skippedCandidateCount: 0,
      failedCandidateCount: 0,
      results: [],
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("does not let force bypass the master New Jersey finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runNewJerseyCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js"
    );

    const result = await runNewJerseyCandidateFinanceSyncJob({
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

  it("runs the due New Jersey finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), connect: vi.fn(), end };
    const syncDueNewJerseyCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: true,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 4,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      skippedCandidateCount: 1,
      failedCandidateCount: 0,
      results: [],
    });

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    vi.doMock("../../src/pipeline/newJerseyFinance/newJerseyCandidateFinanceBatchSync.js", () => ({
      syncDueNewJerseyCandidateFinance,
    }));

    const { runNewJerseyCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js"
    );

    const result = await runNewJerseyCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      electionLookaheadDays: 365,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      skippedCandidateCount: 1,
    });
    expect(syncDueNewJerseyCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        electionLookaheadDays: 365,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "15 9 * * *";
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/New_York";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringNewJerseyCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringNewJerseyCandidateFinanceSyncJobs({
      maxCandidates: 5,
      dryRun: true,
      electionLookbackDays: 2,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "new_jersey_candidate_finance_sync_daily",
      {
        pattern: "15 9 * * *",
        tz: "America/New_York",
      },
      expect.objectContaining({
        name: "new_jersey_candidate_finance_sync_due",
        data: expect.objectContaining({
          dryRun: true,
          maxCandidates: 5,
          electionLookbackDays: 2,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring scheduler when the master New Jersey finance flag is disabled", async () => {
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringNewJerseyCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringNewJerseyCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("new_jersey_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("enqueues manual jobs with a deterministic linked-election job id", async () => {
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "new-jersey-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const {
      buildNewJerseyCandidateFinanceLinkedElectionSyncJobId,
      enqueueManualNewJerseyCandidateFinanceSyncJob,
    } = await import("../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js");

    const jobId = buildNewJerseyCandidateFinanceLinkedElectionSyncJobId(
      new Date("2026-11-03T23:00:00.000Z")
    );
    await expect(
      enqueueManualNewJerseyCandidateFinanceSyncJob(
        { maxCandidates: 3, staleAfterDays: 2 },
        { jobId }
      )
    ).resolves.toBe("new-jersey-finance-job-1");

    expect(jobId).toBe("new-jersey-candidate-finance-linked-election-sync-2026-11-03");
    expect(queueInstance.add).toHaveBeenCalledWith(
      "new_jersey_candidate_finance_sync_due",
      expect.objectContaining({
        maxCandidates: 3,
        staleAfterDays: 2,
        triggeredBy: "manual",
      }),
      expect.objectContaining({ jobId })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects invalid scheduler job options before touching BullMQ", async () => {
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const Queue = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualNewJerseyCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualNewJerseyCandidateFinanceSyncJob({ maxCandidates: 0 })).rejects.toThrow(
      "Invalid New Jersey finance sync scheduler maxCandidates"
    );
    expect(Queue).not.toHaveBeenCalled();
  });

  it("requires explicit Redis and database URLs for enabled background work", async () => {
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    process.env.DATABASE_URL = "postgresql://localhost:5432/test";
    delete process.env.REDIS_URL;
    const Queue = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualNewJerseyCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualNewJerseyCandidateFinanceSyncJob({ maxCandidates: 1 })).rejects.toThrow(
      "REDIS_URL is required for New Jersey candidate finance sync scheduler"
    );
    expect(Queue).not.toHaveBeenCalled();

    vi.resetModules();
    process.env.REDIS_URL = "redis://localhost:6379/0";
    delete process.env.DATABASE_URL;
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runNewJerseyCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/newJerseyCandidateFinanceSyncScheduler.js"
    );

    await expect(runNewJerseyCandidateFinanceSyncJob({ triggeredBy: "manual" })).rejects.toThrow(
      "DATABASE_URL is required for New Jersey candidate finance sync scheduler"
    );
    expect(Pool).not.toHaveBeenCalled();
  });
});
