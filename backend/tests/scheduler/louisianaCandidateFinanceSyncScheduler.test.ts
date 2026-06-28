import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("louisianaCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.LOUISIANA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("returns a disabled no-op result without importing the Louisiana sync pipeline", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.LOUISIANA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";

    const Pool = vi.fn();
    const syncDueLouisianaCandidateFinance = vi.fn();
    vi.doMock("pg", () => ({ Pool }));
    vi.doMock("../../src/pipeline/louisianaFinance/louisianaCandidateFinanceBatchSync.js", () => ({
      syncDueLouisianaCandidateFinance,
    }));

    const { runLouisianaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/louisianaCandidateFinanceSyncScheduler.js"
    );

    const result = await runLouisianaCandidateFinanceSyncJob({
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
    expect(syncDueLouisianaCandidateFinance).not.toHaveBeenCalled();
  });

  it("does not let force bypass the master Louisiana finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.LOUISIANA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const Pool = vi.fn();
    const syncDueLouisianaCandidateFinance = vi.fn();
    vi.doMock("pg", () => ({ Pool }));
    vi.doMock("../../src/pipeline/louisianaFinance/louisianaCandidateFinanceBatchSync.js", () => ({
      syncDueLouisianaCandidateFinance,
    }));

    const { runLouisianaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/louisianaCandidateFinanceSyncScheduler.js"
    );

    const result = await runLouisianaCandidateFinanceSyncJob({
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
    expect(syncDueLouisianaCandidateFinance).not.toHaveBeenCalled();
  });

  it("runs the due Louisiana finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.LOUISIANA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueLouisianaCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: true,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 4,
      selectedCandidateCount: 2,
      syncedCandidateCount: 2,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
      results: [],
    });

    vi.doMock("pg", () => ({ Pool: vi.fn(() => pool) }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));
    vi.doMock("../../src/pipeline/louisianaFinance/louisianaCandidateFinanceBatchSync.js", () => ({
      syncDueLouisianaCandidateFinance,
    }));

    const { runLouisianaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/louisianaCandidateFinanceSyncScheduler.js"
    );

    const result = await runLouisianaCandidateFinanceSyncJob({
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
      autoLinkLinkedCount: 1,
    });
    expect(syncDueLouisianaCandidateFinance).toHaveBeenCalledWith(
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
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.LOUISIANA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON = "15 9 * * *";
    process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ = "America/Chicago";

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));

    const { upsertRecurringLouisianaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/louisianaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringLouisianaCandidateFinanceSyncJobs({ maxCandidates: 5 });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "louisiana_candidate_finance_sync_daily",
      {
        pattern: "15 9 * * *",
        tz: "America/Chicago",
      },
      expect.objectContaining({
        name: "louisiana_candidate_finance_sync_due",
        data: expect.objectContaining({
          maxCandidates: 5,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring scheduler when the master Louisiana finance flag is disabled", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.LOUISIANA_CAMPAIGN_FINANCE_ENABLED = "false";

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));

    const { upsertRecurringLouisianaCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/louisianaCandidateFinanceSyncScheduler.js"
    );

    await upsertRecurringLouisianaCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("louisiana_candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
