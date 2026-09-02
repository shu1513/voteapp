import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("southCarolinaCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
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
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runSouthCarolinaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/southCarolinaCandidateFinanceSyncScheduler.js"
    );

    const result = await runSouthCarolinaCandidateFinanceSyncJob({ triggeredBy: "daily" });

    expect(result).toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      dryRun: false,
      autoLinkResults: [],
      totalDueRows: 0,
      attempted: 0,
      succeeded: 0,
      failed: 0,
      candidates: [],
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("does not let force bypass the master South Carolina finance flag", async () => {
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runSouthCarolinaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/southCarolinaCandidateFinanceSyncScheduler.js"
    );

    const result = await runSouthCarolinaCandidateFinanceSyncJob({ triggeredBy: "manual", force: true });

    expect(result).toMatchObject({ enabled: false, force: true, attempted: 0 });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("runs the due South Carolina finance sync when enabled", async () => {
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueSouthCarolinaCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: true,
      autoLinkResults: [],
      totalDueRows: 4,
      attempted: 2,
      succeeded: 2,
      failed: 0,
      candidates: [],
    });

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    mockEnv();
    vi.doMock(
      "../../src/pipeline/southCarolinaFinance/southCarolinaCandidateFinanceBatchSync.js",
      () => ({ syncDueSouthCarolinaCandidateFinance })
    );

    const { runSouthCarolinaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/southCarolinaCandidateFinanceSyncScheduler.js"
    );

    const result = await runSouthCarolinaCandidateFinanceSyncJob({
      dryRun: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({ enabled: true, triggeredBy: "manual", dryRun: true, attempted: 2 });
    expect(syncDueSouthCarolinaCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({ db: pool, dryRun: true, maxCandidates: 2, staleAfterDays: 3 })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "15 9 * * *";
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/New_York";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringSouthCarolinaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/southCarolinaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringSouthCarolinaCandidateFinanceSyncJobs({ maxCandidates: 5 });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "south_carolina_candidate_finance_sync_daily",
      { pattern: "15 9 * * *", tz: "America/New_York" },
      expect.objectContaining({
        name: "south_carolina_candidate_finance_sync_due",
        data: expect.objectContaining({ maxCandidates: 5, triggeredBy: "daily" }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring scheduler when either flag is disabled", async () => {
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringSouthCarolinaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/southCarolinaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringSouthCarolinaCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "south_carolina_candidate_finance_sync_daily"
    );
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs and reports disabled without touching the queue when off", async () => {
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "sc-finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualSouthCarolinaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/southCarolinaCandidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualSouthCarolinaCandidateFinanceSyncJob({})).resolves.toBe("sc-finance-job-1");
    expect(queueInstance.add).toHaveBeenCalledWith(
      "south_carolina_candidate_finance_sync_due",
      expect.objectContaining({ triggeredBy: "manual" }),
      expect.any(Object)
    );

    process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    vi.resetModules();
    mockEnv();
    const QueueDisabled = vi.fn();
    vi.doMock("bullmq", () => ({ Queue: QueueDisabled, Worker: vi.fn() }));
    const disabledModule = await import(
      "../../src/scheduler/southCarolinaCandidateFinanceSyncScheduler.js"
    );
    await expect(disabledModule.enqueueManualSouthCarolinaCandidateFinanceSyncJob({})).resolves.toBe(
      "disabled"
    );
    expect(QueueDisabled).not.toHaveBeenCalled();
  });
});
