import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("candidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.CANDIDATE_FINANCE_ENABLED;
    delete process.env.CANDIDATE_FINANCE_SYNC_ENABLED;
    delete process.env.FEC_API_KEY_1;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("returns a disabled no-op result when the feature flag is off and not forced", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.CANDIDATE_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runCandidateFinanceSyncJob } = await import("../../src/scheduler/candidateFinanceSyncScheduler.js");

    const result = await runCandidateFinanceSyncJob({
      triggeredBy: "daily",
      maxCandidates: 10,
      staleAfterDays: 7,
    });

    expect(result).toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      dryRun: false,
      includeOutside: false,
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

  it("does not let force bypass the master candidate finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.CANDIDATE_FINANCE_ENABLED = "false";
    process.env.CANDIDATE_FINANCE_SYNC_ENABLED = "true";
    process.env.FEC_API_KEY_1 = "k1";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runCandidateFinanceSyncJob } = await import("../../src/scheduler/candidateFinanceSyncScheduler.js");

    const result = await runCandidateFinanceSyncJob({
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

  it("runs the due finance sync when enabled", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.CANDIDATE_FINANCE_ENABLED = "true";
    process.env.CANDIDATE_FINANCE_SYNC_ENABLED = "true";
    process.env.FEC_API_KEY_1 = "k1";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncDueCandidateFinance = vi.fn().mockResolvedValue({
      dryRun: true,
      includeOutside: true,
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
    vi.doMock("../../src/pipeline/finance/candidateFinanceBatchSync.js", () => ({
      syncDueCandidateFinance,
    }));

    const { runCandidateFinanceSyncJob } = await import("../../src/scheduler/candidateFinanceSyncScheduler.js");

    const result = await runCandidateFinanceSyncJob({
      dryRun: true,
      includeOutside: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      perPage: 10,
      timeoutMs: 5000,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      dryRun: true,
      includeOutside: true,
      selectedCandidateCount: 2,
    });
    expect(syncDueCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        openFecOptions: { apiKeys: ["k1"], timeoutMs: 5000 },
        dryRun: true,
        includeOutside: true,
        maxCandidates: 2,
        staleAfterDays: 3,
        electionLookbackDays: 14,
        perPage: 10,
      })
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("runs a targeted candidate finance sync when a FEC candidate and election year are provided", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.CANDIDATE_FINANCE_ENABLED = "true";
    process.env.CANDIDATE_FINANCE_SYNC_ENABLED = "false";
    process.env.FEC_API_KEY_1 = "k1";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncCandidateFinance = vi.fn().mockResolvedValue({
      fecCandidateId: "S80000001",
      electionYear: 2026,
      dryRun: false,
      directCommitteeCount: 1,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      industryBreakdownsWritten: 0,
      classificationsWritten: 0,
      outsideIncluded: true,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
    });
    const syncDueCandidateFinance = vi.fn();

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
    vi.doMock("../../src/pipeline/finance/candidateFinanceBatchSync.js", () => ({
      syncDueCandidateFinance,
    }));
    vi.doMock("../../src/pipeline/finance/candidateFinanceSync.js", () => ({
      syncCandidateFinance,
    }));

    const { runCandidateFinanceSyncJob } = await import("../../src/scheduler/candidateFinanceSyncScheduler.js");

    const result = await runCandidateFinanceSyncJob({
      force: true,
      includeOutside: true,
      candidateId: "candidate-1",
      fecCandidateId: " s80000001 ",
      electionYear: 2026,
      source: "candidate_election",
      triggeredBy: "manual",
    });

    expect(syncCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        fecCandidateId: "S80000001",
        electionYear: 2026,
        openFecOptions: { apiKeys: ["k1"], timeoutMs: expect.any(Number) },
        includeOutside: true,
        now: new Date("2026-06-01T00:00:00.000Z"),
      })
    );
    expect(syncDueCandidateFinance).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      enabled: true,
      force: true,
      triggeredBy: "manual",
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      results: [
        {
          candidateId: "candidate-1",
          fecCandidateId: "S80000001",
          electionYear: 2026,
          source: "candidate_election",
          ok: true,
        },
      ],
    });
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("preserves presidential-cycle source in targeted finance sync results", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2028-02-01T00:00:00.000Z"));
    process.env.CANDIDATE_FINANCE_ENABLED = "true";
    process.env.CANDIDATE_FINANCE_SYNC_ENABLED = "true";
    process.env.FEC_API_KEY_1 = "k1";

    const end = vi.fn().mockResolvedValue(undefined);
    const pool = { query: vi.fn(), end };
    const syncCandidateFinance = vi.fn().mockResolvedValue({
      fecCandidateId: "P80000001",
      electionYear: 2028,
      dryRun: false,
      directCommitteeCount: 1,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      industryBreakdownsWritten: 0,
      classificationsWritten: 0,
      outsideIncluded: true,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
    });
    const syncDueCandidateFinance = vi.fn();

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
    vi.doMock("../../src/pipeline/finance/candidateFinanceBatchSync.js", () => ({
      syncDueCandidateFinance,
    }));
    vi.doMock("../../src/pipeline/finance/candidateFinanceSync.js", () => ({
      syncCandidateFinance,
    }));

    const { runCandidateFinanceSyncJob } = await import("../../src/scheduler/candidateFinanceSyncScheduler.js");

    const result = await runCandidateFinanceSyncJob({
      includeOutside: true,
      candidateId: "candidate-president",
      fecCandidateId: "p80000001",
      electionYear: 2028,
      source: "presidential_cycle",
      triggeredBy: "candidate_link",
    });

    expect(syncCandidateFinance).toHaveBeenCalledWith(
      expect.objectContaining({
        db: pool,
        fecCandidateId: "P80000001",
        electionYear: 2028,
        includeOutside: true,
      })
    );
    expect(syncDueCandidateFinance).not.toHaveBeenCalled();
    expect(result.results).toEqual([
      expect.objectContaining({
        candidateId: "candidate-president",
        fecCandidateId: "P80000001",
        electionYear: 2028,
        source: "presidential_cycle",
        ok: true,
      }),
    ]);
    expect(end).toHaveBeenCalledTimes(1);
  });

  it("upserts recurring jobs with configured queue payload", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.CANDIDATE_FINANCE_ENABLED = "true";
    process.env.CANDIDATE_FINANCE_SYNC_DAILY_CRON = "5 9 * * *";
    process.env.CANDIDATE_FINANCE_SYNC_DAILY_TZ = "America/Los_Angeles";

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

    const { upsertRecurringCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await upsertRecurringCandidateFinanceSyncJobs({
      includeOutside: true,
      maxCandidates: 5,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "candidate_finance_sync_daily",
      {
        pattern: "5 9 * * *",
        tz: "America/Los_Angeles",
      },
      expect.objectContaining({
        name: "candidate_finance_sync_due",
        data: expect.objectContaining({
          includeOutside: true,
          maxCandidates: 5,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects malformed Redis DB path segments", async () => {
    process.env.CANDIDATE_FINANCE_ENABLED = "true";

    const Queue = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0/foo",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));

    const { upsertRecurringCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await expect(upsertRecurringCandidateFinanceSyncJobs()).rejects.toThrow("Invalid REDIS_URL db index");
    expect(Queue).not.toHaveBeenCalled();
  });

  it("removes the recurring scheduler when the master finance flag is disabled", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.CANDIDATE_FINANCE_ENABLED = "false";

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

    const { upsertRecurringCandidateFinanceSyncJobs } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await upsertRecurringCandidateFinanceSyncJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("candidate_finance_sync_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("does not enqueue manual jobs when the master finance flag is disabled", async () => {
    process.env.CANDIDATE_FINANCE_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "finance-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await expect(enqueueManualCandidateFinanceSyncJob({ force: true })).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
    expect(queueInstance.add).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs with requested options", async () => {
    process.env.CANDIDATE_FINANCE_ENABLED = "true";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "finance-job-1" }),
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

    const { enqueueManualCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualCandidateFinanceSyncJob({
        dryRun: true,
        force: true,
        includeOutside: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
      })
    ).resolves.toBe("finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "candidate_finance_sync_due",
      expect.objectContaining({
        dryRun: true,
        force: true,
        includeOutside: true,
        maxCandidates: 3,
        electionLookbackDays: 21,
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("enqueues manual targeted jobs with FEC candidate and election year", async () => {
    process.env.CANDIDATE_FINANCE_ENABLED = "true";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "finance-job-1" }),
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

    const { enqueueManualCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueManualCandidateFinanceSyncJob({
        force: true,
        includeOutside: true,
        candidateId: "candidate-1",
        fecCandidateId: " s80000001 ",
        electionYear: 2026,
      })
    ).resolves.toBe("finance-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "candidate_finance_sync_due",
      expect.objectContaining({
        force: true,
        includeOutside: true,
        candidateId: "candidate-1",
        fecCandidateId: "S80000001",
        electionYear: 2026,
        triggeredBy: "manual",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("rejects incomplete targeted job options", async () => {
    const { runCandidateFinanceSyncJob } = await import("../../src/scheduler/candidateFinanceSyncScheduler.js");

    await expect(
      runCandidateFinanceSyncJob({
        fecCandidateId: "S80000001",
        triggeredBy: "manual",
      })
    ).rejects.toThrow("targeted jobs require both fecCandidateId and electionYear");
    await expect(
      runCandidateFinanceSyncJob({
        electionYear: 2026,
        triggeredBy: "manual",
      })
    ).rejects.toThrow("targeted jobs require both fecCandidateId and electionYear");
  });

  it("enqueues targeted candidate-link jobs", async () => {
    process.env.CANDIDATE_FINANCE_ENABLED = "true";
    process.env.CANDIDATE_FINANCE_SYNC_ENABLED = "true";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "finance-link-job-1" }),
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

    const { enqueueCandidateLinkCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueCandidateLinkCandidateFinanceSyncJob({
        candidateId: "candidate-1",
        fecCandidateId: "h80000001",
        electionYear: 2026,
        includeOutside: true,
      })
    ).resolves.toBe("finance-link-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "candidate_finance_sync_due",
      expect.objectContaining({
        candidateId: "candidate-1",
        fecCandidateId: "H80000001",
        electionYear: 2026,
        source: "candidate_election",
        force: false,
        includeOutside: true,
        triggeredBy: "candidate_link",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("does not enqueue targeted candidate-link jobs when finance sync is disabled", async () => {
    process.env.CANDIDATE_FINANCE_ENABLED = "true";
    process.env.CANDIDATE_FINANCE_SYNC_ENABLED = "false";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "finance-link-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueCandidateLinkCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueCandidateLinkCandidateFinanceSyncJob({
        candidateId: "candidate-1",
        fecCandidateId: "H80000001",
        electionYear: 2026,
      })
    ).resolves.toBe("disabled");

    expect(Queue).not.toHaveBeenCalled();
    expect(queueInstance.add).not.toHaveBeenCalled();
  });

  it("enqueues targeted presidential-cycle jobs", async () => {
    process.env.CANDIDATE_FINANCE_ENABLED = "true";
    process.env.CANDIDATE_FINANCE_SYNC_ENABLED = "true";

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "finance-presidential-job-1" }),
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

    const { enqueueCandidateLinkCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/candidateFinanceSyncScheduler.js"
    );

    await expect(
      enqueueCandidateLinkCandidateFinanceSyncJob({
        candidateId: "candidate-1",
        fecCandidateId: "p80000001",
        electionYear: 2028,
        source: "presidential_cycle",
        includeOutside: true,
      })
    ).resolves.toBe("finance-presidential-job-1");

    expect(queueInstance.add).toHaveBeenCalledWith(
      "candidate_finance_sync_due",
      expect.objectContaining({
        candidateId: "candidate-1",
        fecCandidateId: "P80000001",
        electionYear: 2028,
        source: "presidential_cycle",
        force: false,
        includeOutside: true,
        triggeredBy: "candidate_link",
      }),
      expect.any(Object)
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
