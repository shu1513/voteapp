import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("texasTecRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED;
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

  it("upserts the daily recurring job", async () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_TEC_RAW_DATA_REFRESH_DAILY_CRON = "35 8 * * *";
    process.env.TEXAS_TEC_RAW_DATA_REFRESH_DAILY_TZ = "America/Chicago";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringTexasTecRawDataRefreshJobs } = await import(
      "../../src/scheduler/texasTecRawDataRefreshScheduler.js"
    );

    await upsertRecurringTexasTecRawDataRefreshJobs({ cacheDir: "/tmp/tx-tec", timeoutMs: 5000 });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(1);
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "texas_tec_raw_data_refresh_daily",
      { pattern: "35 8 * * *", tz: "America/Chicago" },
      expect.objectContaining({
        name: "texas_tec_raw_data_refresh",
        data: expect.objectContaining({
          cacheDir: "/tmp/tx-tec",
          timeoutMs: 5000,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring job when disabled", async () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringTexasTecRawDataRefreshJobs } = await import(
      "../../src/scheduler/texasTecRawDataRefreshScheduler.js"
    );

    await upsertRecurringTexasTecRawDataRefreshJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("texas_tec_raw_data_refresh_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("runs a refresh job when enabled", async () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshTexasTecCsvDatabaseArtifactCache = vi.fn().mockResolvedValue({
      status: "unchanged",
      cacheDir: "/tmp/tx-tec",
      zipPath: "/tmp/tx-tec/TEC_CF_CSV.zip",
      metadataPath: "/tmp/tx-tec/TEC_CF_CSV.metadata.json",
      remote: { url: "https://example.test", contentLength: null, contentType: null, etag: null, lastModified: null },
      previous: null,
      current: {},
    });
    vi.doMock("../../src/pipeline/texasFinance/texasTecCsvDatabaseArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshTexasTecCsvDatabaseArtifactCache,
    }));

    const { runTexasTecRawDataRefreshJob } = await import(
      "../../src/scheduler/texasTecRawDataRefreshScheduler.js"
    );

    const result = await runTexasTecRawDataRefreshJob({
      triggeredBy: "manual",
      cacheDir: "/tmp/tx-tec",
      timeoutMs: 5000,
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      status: "unchanged",
    });
    expect(refreshTexasTecCsvDatabaseArtifactCache).toHaveBeenCalledWith(
      expect.objectContaining({ cacheDir: "/tmp/tx-tec", timeoutMs: 5000 })
    );
  });

  it("does not run refresh when disabled", async () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshTexasTecCsvDatabaseArtifactCache = vi.fn();
    vi.doMock("../../src/pipeline/texasFinance/texasTecCsvDatabaseArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshTexasTecCsvDatabaseArtifactCache,
    }));

    const { runTexasTecRawDataRefreshJob } = await import(
      "../../src/scheduler/texasTecRawDataRefreshScheduler.js"
    );

    await expect(runTexasTecRawDataRefreshJob({ triggeredBy: "daily" })).resolves.toEqual(
      expect.objectContaining({
        enabled: false,
        triggeredBy: "daily",
        status: "disabled",
        refresh: null,
      })
    );
    expect(refreshTexasTecCsvDatabaseArtifactCache).not.toHaveBeenCalled();
  });

  it("returns disabled for manual enqueue when flags are off", async () => {
    process.env.TEXAS_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.TEXAS_TEC_RAW_DATA_REFRESH_ENABLED = "true";
    mockEnv();

    const Queue = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualTexasTecRawDataRefreshJob } = await import(
      "../../src/scheduler/texasTecRawDataRefreshScheduler.js"
    );

    await expect(enqueueManualTexasTecRawDataRefreshJob()).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
  });
});
