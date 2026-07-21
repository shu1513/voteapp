import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("newMexicoCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED;
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
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runNewMexicoCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/newMexicoCandidateFinanceSyncScheduler.js"
    );

    const result = await runNewMexicoCandidateFinanceSyncJob({
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

  it("runs the due New Mexico finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueNewMexicoCandidateFinance = vi.fn().mockResolvedValue({
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
    vi.doMock("../../src/pipeline/newMexicoFinance/newMexicoCandidateFinanceBatchSync.js", () => ({
      syncDueNewMexicoCandidateFinance,
    }));

    const { runNewMexicoCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/newMexicoCandidateFinanceSyncScheduler.js"
    );

    const result = await runNewMexicoCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      rawDataCacheDir: "/tmp/cfis",
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      selectedCandidateCount: 2,
    });
    expect(syncDueNewMexicoCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        rawDataCacheDir: "/tmp/cfis",
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring scheduler when the master New Mexico finance flag is disabled", async () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringNewMexicoCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/newMexicoCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringNewMexicoCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("new_mexico_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs with a deterministic linked-election job id", async () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "new-mexico-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const {
      buildNewMexicoCandidateFinanceLinkedElectionSyncJobId,
      enqueueManualNewMexicoCandidateFinanceSyncJob,
    } = await import("../../src/scheduler/newMexicoCandidateFinanceSyncScheduler.js");

    const jobId = buildNewMexicoCandidateFinanceLinkedElectionSyncJobId(
      new Date("2026-11-03T23:00:00.000Z")
    );
    await expect(
      enqueueManualNewMexicoCandidateFinanceSyncJob(
        {},
        { jobId }
      )
    ).resolves.toBe("new-mexico-finance-job-1");

    expect(jobId).toBe("new-mexico-candidate-finance-linked-election-sync-2026-11-03");
    expect(queueInstance.add).toHaveBeenCalledWith(
      "new_mexico_candidate_finance_sync_due",
      expect.objectContaining({
        triggeredBy: "manual",
      }),
      expect.objectContaining({ jobId })
    );
  });
});
