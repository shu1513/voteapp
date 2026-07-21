import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("floridaCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("returns disabled without creating a queue when the sync flag is off", async () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Queue = vi.fn();
    const Worker = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker }));

    const { enqueueManualFloridaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/floridaCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualFloridaCandidateFinanceSyncJob({ triggeredBy: "manual" })).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
  });

  it("enqueues a manual Florida finance sync job when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T12:00:00.000Z"));
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const add = vi.fn().mockResolvedValue({ id: "fl-job-1" });
    const close = vi.fn().mockResolvedValue(undefined);
    const Queue = vi.fn(() => ({ add, close }));
    const Worker = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        REDIS_URL: "redis://localhost:6379/3",
      }),
    }));

    const {
      buildFloridaCandidateFinanceLinkedElectionSyncJobId,
      enqueueManualFloridaCandidateFinanceSyncJob,
    } = await import("../../src/scheduler/floridaCandidateFinanceSyncScheduler.js");

    const jobId = buildFloridaCandidateFinanceLinkedElectionSyncJobId();
    await expect(
      enqueueManualFloridaCandidateFinanceSyncJob(
        {
          triggeredBy: "manual",
        },
        { jobId }
      )
    ).resolves.toBe("fl-job-1");

    expect(jobId).toBe("florida-candidate-finance-linked-election-sync-2026-06-01");
    expect(Queue).toHaveBeenCalledWith(
      "florida_candidate_finance_sync_maintenance",
      expect.objectContaining({
        connection: expect.objectContaining({
          host: "localhost",
          port: 6379,
          db: 3,
        }),
      })
    );
    expect(add).toHaveBeenCalledWith(
      "florida_candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: false,
        force: false,
        maxCandidates: undefined,
        staleAfterDays: undefined,
        electionLookbackDays: undefined,
        electionLookaheadDays: undefined,
        syncInputs: undefined,
        defaultArtifactCacheDir: undefined,
        refreshExportArtifacts: false,
        exportMinIntervalMs: undefined,
        exportRowLimit: undefined,
        triggeredBy: "manual",
        requestedAt: "2026-06-01T12:00:00.000Z",
      }),
      expect.objectContaining({
        jobId,
      })
    );
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("validates job ids and numeric options", async () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Queue = vi.fn(() => ({
      add: vi.fn().mockResolvedValue({ id: "unused" }),
      close: vi.fn().mockResolvedValue(undefined),
    }));
    const Worker = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        REDIS_URL: "redis://localhost:6379/3",
      }),
    }));

    const { enqueueManualFloridaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/floridaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualFloridaCandidateFinanceSyncJob({ maxCandidates: 0 })
    ).rejects.toThrow("Invalid Florida finance sync scheduler maxCandidates");
    await expect(
      enqueueManualFloridaCandidateFinanceSyncJob({ staleAfterDays: 0, electionLookbackDays: 0, electionLookaheadDays: 0 })
    ).resolves.toBe("unused");
    await expect(
      enqueueManualFloridaCandidateFinanceSyncJob({}, { jobId: "bad:id" })
    ).rejects.toThrow("Florida finance sync scheduler jobId must not contain ':'");
  });

  it("upserts and removes the recurring BullMQ scheduler", async () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "5 10 * * *";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/New_York";

    const upsertJobScheduler = vi.fn().mockResolvedValue(undefined);
    const removeJobScheduler = vi.fn().mockResolvedValue(undefined);
    const close = vi.fn().mockResolvedValue(undefined);
    const Queue = vi.fn(() => ({ upsertJobScheduler, removeJobScheduler, close }));
    const Worker = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        REDIS_URL: "redis://localhost:6379/3",
      }),
    }));

    const { upsertRecurringFloridaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/floridaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringFloridaCandidateFinanceSyncJobs({
      dryRun: true,
      maxCandidates: 2,
    });

    expect(upsertJobScheduler).toHaveBeenCalledWith(
      "florida_candidate_finance_sync_daily",
      {
        pattern: "5 10 * * *",
        tz: "America/New_York",
      },
      {
        name: "florida_candidate_finance_sync_due",
        data: expect.objectContaining({
          dryRun: true,
          force: false,
          maxCandidates: 2,
          triggeredBy: "daily",
        }),
        opts: expect.objectContaining({
          removeOnComplete: 1000,
          removeOnFail: 1000,
        }),
      }
    );

    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "false";
    await upsertRecurringFloridaCandidateFinanceSyncJobs();
    expect(removeJobScheduler).toHaveBeenCalledWith("florida_candidate_finance_sync_daily");
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    await upsertRecurringFloridaCandidateFinanceSyncJobs();
    expect(removeJobScheduler).toHaveBeenCalledTimes(2);
    await upsertRecurringFloridaCandidateFinanceSyncJobs({ force: true });
    expect(upsertJobScheduler).toHaveBeenCalledTimes(2);
    expect(close).toHaveBeenCalledTimes(4);
  });

  it("runs an enabled job through the Florida batch sync", async () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const pool = { end: vi.fn().mockResolvedValue(undefined) };
    const Pool = vi.fn(() => pool);
    const syncFloridaCandidateFinanceBatch = vi.fn().mockResolvedValue({
      dryRun: false,
      now: "2026-06-01T12:00:00.000Z",
      maxCandidates: 1,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
      results: [],
    });
    const syncDueFloridaCandidateFinance = vi.fn();
    const Queue = vi.fn();
    const Worker = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker }));
    vi.doMock("pg", () => ({ Pool }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgres://test/florida",
        REDIS_URL: "redis://localhost:6379/3",
      }),
    }));
    vi.doMock("../../src/pipeline/floridaFinance/floridaCandidateFinanceBatchSync.js", () => ({
      syncFloridaCandidateFinanceBatch,
      syncDueFloridaCandidateFinance,
    }));

    const { runFloridaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/floridaCandidateFinanceSyncScheduler.js"
    );

    const syncInputs = [
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
        trustedCommittee: {
          committeeId: "FRIENDS_OF_JANE_DOE",
          committeeName: "Friends of Jane Doe",
        },
      },
    ];
    await expect(
      runFloridaCandidateFinanceSyncJob({
        maxCandidates: 1,
        syncInputs,
        triggeredBy: "manual",
      })
    ).resolves.toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      syncedCandidateCount: 1,
    });

    expect(Pool).toHaveBeenCalledWith({ connectionString: "postgres://test/florida" });
    expect(syncFloridaCandidateFinanceBatch).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: false,
        maxCandidates: 1,
        syncInputs,
      })
    );
    expect(pool.end).toHaveBeenCalledTimes(1);
  });

  it("runs an enabled job through Florida due sync when no explicit inputs are provided", async () => {
    process.env.FLORIDA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const pool = { end: vi.fn().mockResolvedValue(undefined) };
    const Pool = vi.fn(() => pool);
    const syncFloridaCandidateFinanceBatch = vi.fn();
    const syncDueFloridaCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: false,
      now: "2026-06-01T12:00:00.000Z",
      staleAfterDays: 7,
      maxCandidates: 1,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      failedCandidateCount: 0,
      results: [],
    });
    const Queue = vi.fn();
    const Worker = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker }));
    vi.doMock("pg", () => ({ Pool }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgres://test/florida",
        REDIS_URL: "redis://localhost:6379/3",
      }),
    }));
    vi.doMock("../../src/pipeline/floridaFinance/floridaCandidateFinanceBatchSync.js", () => ({
      syncFloridaCandidateFinanceBatch,
      syncDueFloridaCandidateFinance,
    }));

    const { runFloridaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/floridaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      runFloridaCandidateFinanceSyncJob({
        maxCandidates: 1,
        staleAfterDays: 5,
        electionLookbackDays: 2,
        electionLookaheadDays: 90,
        exportMinIntervalMs: 0,
        exportRowLimit: 5000,
        refreshExportArtifacts: true,
        force: true,
        triggeredBy: "daily",
      })
    ).resolves.toMatchObject({
      enabled: true,
      force: true,
      triggeredBy: "daily",
      dueCandidateCount: 0,
    });

    expect(syncFloridaCandidateFinanceBatch).not.toHaveBeenCalled();
    expect(syncDueFloridaCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: false,
        maxCandidates: 1,
        staleAfterDays: 5,
        electionLookbackDays: 2,
        electionLookaheadDays: 90,
        exportMinIntervalMs: 0,
        exportRowLimit: 5000,
        exportForce: true,
        refreshExportArtifacts: true,
      })
    );
    expect(pool.end).toHaveBeenCalledTimes(1);
  });

  it("creates a BullMQ worker that processes Florida sync jobs", async () => {
    const Queue = vi.fn();
    const Worker = vi.fn((_queueName, processor, _options) => ({ processor }));
    vi.doMock("bullmq", () => ({ Queue, Worker }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        REDIS_URL: "redis://localhost:6379/3",
      }),
    }));

    const { createFloridaCandidateFinanceSyncSchedulerWorker } = await import(
      "../../src/scheduler/floridaCandidateFinanceSyncScheduler.js"
    );

    const worker = createFloridaCandidateFinanceSyncSchedulerWorker() as unknown as {
      processor: (job: { data: Record<string, unknown> }) => Promise<unknown>;
    };

    expect(Worker).toHaveBeenCalledWith(
      "florida_candidate_finance_sync_maintenance",
      expect.any(Function),
      expect.objectContaining({
        connection: expect.objectContaining({
          host: "localhost",
          port: 6379,
          db: 3,
        }),
        concurrency: 1,
      })
    );
    expect(worker.processor).toEqual(expect.any(Function));
  });
});
