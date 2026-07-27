import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("michiganCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.MICHIGAN_CAMPAIGN_FINANCE_SYNC_ENABLED;
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
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MICHIGAN_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runMichiganCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/michiganCandidateFinanceSyncScheduler.js"
    );

    const result = await runMichiganCandidateFinanceSyncJob({
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

  it("does not let force bypass the master Michigan finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.MICHIGAN_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runMichiganCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/michiganCandidateFinanceSyncScheduler.js"
    );

    const result = await runMichiganCandidateFinanceSyncJob({
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

  it("runs the due Michigan finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MICHIGAN_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueMichiganCandidateFinance = vi.fn().mockResolvedValue({
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
    vi.doMock("../../src/pipeline/michiganFinance/michiganCandidateFinanceBatchSync.js", () => ({
      syncDueMichiganCandidateFinance,
    }));

    const { runMichiganCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/michiganCandidateFinanceSyncScheduler.js"
    );

    const result = await runMichiganCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      selectedCandidateCount: 2,
    });
    expect(syncDueMichiganCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        dryRun: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MICHIGAN_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "30 9 * * *";
    process.env.MICHIGAN_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/Detroit";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMichiganCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/michiganCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringMichiganCandidateFinanceSyncJobs({
      maxCandidates: 5,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "michigan_candidate_finance_sync_daily",
      {
        pattern: "30 9 * * *",
        tz: "America/Detroit",
      },
      expect.objectContaining({
        name: "michigan_candidate_finance_sync_due",
        data: expect.objectContaining({
          maxCandidates: 5,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes recurring jobs when the master Michigan finance flag is disabled", async () => {
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMichiganCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/michiganCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringMichiganCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("michigan_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("enqueues manual jobs with requested options", async () => {
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MICHIGAN_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "michigan-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualMichiganCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/michiganCandidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualMichiganCandidateFinanceSyncJob({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
      })
    ).resolves.toBe("michigan-finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "michigan_candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: true,
        force: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
