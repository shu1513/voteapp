import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Behavior shared with the state schedulers (gating, payloads, close()
// cleanup) is pinned by each migrated state's own test file. This file pins
// what only the factory decides: how config strings reach the queue, the env
// override keys, the message labels, and the input validation.

describe("createStateCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.EXAMPLE_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE;
    delete process.env.EXAMPLE_CAMPAIGN_FINANCE_SYNC_DAILY_CRON;
    delete process.env.EXAMPLE_CAMPAIGN_FINANCE_SYNC_DAILY_TZ;
  });

  afterEach(() => {
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  function mockEnv() {
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0",
      }),
    }));
  }

  async function createScheduler(overrides: { isEnabled?: () => boolean; isSyncEnabled?: (force: boolean) => boolean } = {}) {
    const { createStateCandidateFinanceSyncScheduler } = await import(
      "../../src/scheduler/stateCandidateFinanceSyncScheduler.js"
    );
    const syncDue = vi.fn();
    const scheduler = createStateCandidateFinanceSyncScheduler({
      stateLabel: "Example",
      jobName: "example_candidate_finance_sync_due",
      dailySchedulerId: "example_candidate_finance_sync_daily",
      defaultQueueName: "example_candidate_finance_sync_maintenance",
      linkedElectionJobIdPrefix: "example-candidate-finance-linked-election-sync-",
      envPrefix: "EXAMPLE_CAMPAIGN_FINANCE_SYNC",
      defaultDailyCron: "25 9 * * *",
      isEnabled: overrides.isEnabled ?? (() => true),
      isSyncEnabled: overrides.isSyncEnabled ?? (() => true),
      syncDue,
    });
    return { scheduler, syncDue };
  }

  it("names the queue from config and falls back to the default cron and UTC", async () => {
    mockEnv();
    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { scheduler } = await createScheduler();
    await scheduler.upsertRecurringJobs({ maxCandidates: 5 });

    expect(Queue).toHaveBeenCalledWith(
      "example_candidate_finance_sync_maintenance",
      expect.objectContaining({ defaultJobOptions: { removeOnComplete: 1000, removeOnFail: 1000 } })
    );
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "example_candidate_finance_sync_daily",
      { pattern: "25 9 * * *", tz: "UTC" },
      {
        name: "example_candidate_finance_sync_due",
        data: {
          dryRun: false,
          force: false,
          maxCandidates: 5,
          staleAfterDays: undefined,
          electionLookbackDays: undefined,
          electionLookaheadDays: undefined,
          triggeredBy: "daily",
        },
        opts: { removeOnComplete: 1000, removeOnFail: 1000 },
      }
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("reads the queue, cron and tz overrides from the env prefix", async () => {
    process.env.EXAMPLE_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE = " example_override_queue ";
    process.env.EXAMPLE_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "15 9 * * *";
    process.env.EXAMPLE_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/New_York";
    mockEnv();
    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    const Worker = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker }));

    const { scheduler } = await createScheduler();
    await scheduler.upsertRecurringJobs();
    scheduler.createWorker();

    expect(Queue).toHaveBeenCalledWith("example_override_queue", expect.anything());
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "example_candidate_finance_sync_daily",
      { pattern: "15 9 * * *", tz: "America/New_York" },
      expect.anything()
    );
    expect(Worker).toHaveBeenCalledWith("example_override_queue", expect.any(Function), {
      connection: expect.anything(),
      concurrency: 1,
    });
  });

  it("labels validation errors with the state and rejects unsafe integers and ':' job ids", async () => {
    mockEnv();
    // The queue is opened before the job id is validated (and closed in the
    // finally), so the ':' case needs a closable queue.
    const queueInstance = { add: vi.fn(), close: vi.fn().mockResolvedValue(undefined) };
    vi.doMock("bullmq", () => ({ Queue: vi.fn(() => queueInstance), Worker: vi.fn() }));
    const { scheduler } = await createScheduler();

    await expect(scheduler.runJob({ maxCandidates: 0 })).rejects.toThrow(
      "Invalid Example finance sync scheduler maxCandidates: 0"
    );
    await expect(scheduler.upsertRecurringJobs({ staleAfterDays: Number("9007199254740993") })).rejects.toThrow(
      "Invalid Example finance sync scheduler staleAfterDays: 9007199254740992"
    );
    await expect(scheduler.enqueueManualJob({ electionLookbackDays: 1.5 })).rejects.toThrow(
      "Invalid Example finance sync scheduler electionLookbackDays: 1.5"
    );
    await expect(scheduler.enqueueManualJob({}, { jobId: "a:b" })).rejects.toThrow(
      "Example finance sync scheduler jobId must not contain ':'"
    );
    expect(queueInstance.add).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
    expect(() => scheduler.buildLinkedElectionSyncJobId(new Date("nope"))).toThrow(
      "Invalid Example finance linked-election sync job date"
    );
    expect(scheduler.buildLinkedElectionSyncJobId(new Date("2026-11-03T23:00:00.000Z"))).toBe(
      "example-candidate-finance-linked-election-sync-2026-11-03"
    );
  });

  it("passes the pool and job fields to syncDue and closes the pool", async () => {
    mockEnv();
    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { end };
    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    vi.doMock("bullmq", () => ({ Queue: vi.fn(), Worker: vi.fn() }));
    const isSyncEnabled = vi.fn(() => true);
    const { scheduler, syncDue } = await createScheduler({ isSyncEnabled });
    syncDue.mockResolvedValue({
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

    const result = await scheduler.runJob({ dryRun: true, force: true, maxCandidates: 2, staleAfterDays: 3, triggeredBy: "manual" });

    expect(isSyncEnabled).toHaveBeenCalledWith(true);
    expect(syncDue).toHaveBeenCalledWith({
      db: pool,
      now: expect.any(Date),
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
    });
    expect(result).toMatchObject({ enabled: true, force: true, triggeredBy: "manual", selectedCandidateCount: 2 });
    expect(end).toHaveBeenCalledTimes(1);
  });
});
