import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("californiaCampaignFinanceRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;
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

  it("returns a disabled no-op result when raw refresh is not enabled", async () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "false";

    const refreshCalAccessRawDataArtifactCache = vi.fn();
    vi.doMock("../../src/pipeline/californiaFinance/calAccessRawDataArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/californiaFinance/calAccessRawDataArtifactCache.js"
      );
      return { ...actual, refreshCalAccessRawDataArtifactCache };
    });

    const { runCaliforniaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/californiaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await expect(runCaliforniaCampaignFinanceRawDataRefreshJob({ triggeredBy: "daily" })).resolves.toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      status: "disabled",
      refresh: null,
    });
    expect(refreshCalAccessRawDataArtifactCache).not.toHaveBeenCalled();
  });

  it("runs the refresh cache when enabled", async () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "true";

    const refresh = {
      status: "unchanged",
      cacheDir: "/cache",
      zipPath: "/cache/dbwebexport.zip",
      metadataPath: "/cache/dbwebexport.metadata.json",
      remote: { url: "https://example.test/db.zip", contentLength: 1, contentType: "zip", etag: "e", lastModified: "d" },
      previous: null,
      current: {
        version: 1,
        zipPath: "/cache/dbwebexport.zip",
        metadataPath: "/cache/dbwebexport.metadata.json",
        downloadedAt: "2026-06-19T00:00:00.000Z",
        remote: { url: "https://example.test/db.zip", contentLength: 1, contentType: "zip", etag: "e", lastModified: "d" },
        bytesWritten: 1,
      },
    };
    const refreshCalAccessRawDataArtifactCache = vi.fn().mockResolvedValue(refresh);
    vi.doMock("../../src/pipeline/californiaFinance/calAccessRawDataArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/californiaFinance/calAccessRawDataArtifactCache.js"
      );
      return { ...actual, refreshCalAccessRawDataArtifactCache };
    });

    const { runCaliforniaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/californiaCampaignFinanceRawDataRefreshScheduler.js"
    );

    const result = await runCaliforniaCampaignFinanceRawDataRefreshJob({
      force: true,
      triggeredBy: "manual",
      url: "https://example.test/db.zip",
      cacheDir: "/cache",
      timeoutMs: 5000,
    });

    expect(result).toEqual({
      enabled: true,
      force: true,
      triggeredBy: "manual",
      status: "unchanged",
      refresh,
    });
    expect(refreshCalAccessRawDataArtifactCache).toHaveBeenCalledWith({
      cacheDir: "/cache",
      url: "https://example.test/db.zip",
      force: true,
      timeoutMs: 5000,
    });
  });

  it("upserts a daily scheduler with metadata-check defaults", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_CRON = "20 8 * * *";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_TZ = "America/Los_Angeles";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringCaliforniaCampaignFinanceRawDataRefreshJobs } = await import(
      "../../src/scheduler/californiaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await upsertRecurringCaliforniaCampaignFinanceRawDataRefreshJobs({ cacheDir: "/cache" });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "california_campaign_finance_raw_data_refresh_daily",
      { pattern: "20 8 * * *", tz: "America/Los_Angeles" },
      expect.objectContaining({
        name: "california_campaign_finance_raw_data_refresh",
        data: expect.objectContaining({
          cacheDir: "/cache",
          force: false,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the daily scheduler when the master California finance flag is disabled", async () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringCaliforniaCampaignFinanceRawDataRefreshJobs } = await import(
      "../../src/scheduler/californiaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await upsertRecurringCaliforniaCampaignFinanceRawDataRefreshJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "california_campaign_finance_raw_data_refresh_daily"
    );
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs when raw refresh is enabled or forced", async () => {
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "raw-refresh-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualCaliforniaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/californiaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await expect(enqueueManualCaliforniaCampaignFinanceRawDataRefreshJob()).resolves.toBe("disabled");
    await expect(enqueueManualCaliforniaCampaignFinanceRawDataRefreshJob({ force: true })).resolves.toBe(
      "raw-refresh-job-1"
    );
    expect(queueInstance.add).toHaveBeenCalledWith(
      "california_campaign_finance_raw_data_refresh",
      expect.objectContaining({ force: true, triggeredBy: "manual" }),
      expect.any(Object)
    );
  });
});
