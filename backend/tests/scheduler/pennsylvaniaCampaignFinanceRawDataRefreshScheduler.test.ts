import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const MOCK_PA_EXPORT_URL = "https://www.pa.gov/example/2026.zip";

describe("pennsylvaniaCampaignFinanceRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;
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
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "false";

    const refreshPennsylvaniaCampaignFinanceExportCache = vi.fn();
    vi.doMock("../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceArtifactCache.js"
      );
      return { ...actual, refreshPennsylvaniaCampaignFinanceExportCache };
    });

    const { runPennsylvaniaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/pennsylvaniaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await expect(runPennsylvaniaCampaignFinanceRawDataRefreshJob({ year: 2026, triggeredBy: "daily" })).resolves.toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      status: "disabled",
      refresh: null,
    });
    expect(refreshPennsylvaniaCampaignFinanceExportCache).not.toHaveBeenCalled();
  });

  it("runs the refresh cache when enabled", async () => {
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "true";

    const refresh = {
      status: "unchanged",
      cacheDir: "/cache",
      archivePath: "/cache/2026.zip",
      extractedDir: "/cache/2026",
      metadataPath: "/cache/2026.metadata.json",
      remote: { year: 2026, url: MOCK_PA_EXPORT_URL, contentLength: 1, contentType: "zip", etag: "e", lastModified: "d" },
      previous: null,
      current: {
        version: 1,
        year: 2026,
        archivePath: "/cache/2026.zip",
        extractedDir: "/cache/2026",
        metadataPath: "/cache/2026.metadata.json",
        downloadedAt: "2026-06-19T00:00:00.000Z",
        remote: { year: 2026, url: MOCK_PA_EXPORT_URL, contentLength: 1, contentType: "zip", etag: "e", lastModified: "d" },
        bytesWritten: 1,
      },
    };
    const refreshPennsylvaniaCampaignFinanceExportCache = vi.fn().mockResolvedValue(refresh);
    vi.doMock("../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceArtifactCache.js"
      );
      return { ...actual, refreshPennsylvaniaCampaignFinanceExportCache };
    });

    const { runPennsylvaniaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/pennsylvaniaCampaignFinanceRawDataRefreshScheduler.js"
    );

    const result = await runPennsylvaniaCampaignFinanceRawDataRefreshJob({
      year: 2026,
      force: true,
      triggeredBy: "manual",
      url: MOCK_PA_EXPORT_URL,
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
    expect(refreshPennsylvaniaCampaignFinanceExportCache).toHaveBeenCalledWith({
      year: 2026,
      cacheDir: "/cache",
      url: MOCK_PA_EXPORT_URL,
      force: true,
      timeoutMs: 5000,
    });
  });

  it("upserts a daily scheduler with metadata-check defaults", async () => {
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_CRON = "20 8 * * *";
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_TZ = "America/New_York";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringPennsylvaniaCampaignFinanceRawDataRefreshJobs } = await import(
      "../../src/scheduler/pennsylvaniaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await upsertRecurringPennsylvaniaCampaignFinanceRawDataRefreshJobs({ year: 2026, cacheDir: "/cache" });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "pennsylvania_campaign_finance_raw_data_refresh_daily",
      { pattern: "20 8 * * *", tz: "America/New_York" },
      expect.objectContaining({
        name: "pennsylvania_campaign_finance_raw_data_refresh",
        data: expect.objectContaining({
          year: 2026,
          cacheDir: "/cache",
          force: false,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the daily scheduler when the master Pennsylvania finance flag is disabled", async () => {
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringPennsylvaniaCampaignFinanceRawDataRefreshJobs } = await import(
      "../../src/scheduler/pennsylvaniaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await upsertRecurringPennsylvaniaCampaignFinanceRawDataRefreshJobs({ year: 2026 });

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "pennsylvania_campaign_finance_raw_data_refresh_daily"
    );
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs when raw refresh is enabled or forced", async () => {
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "pa-raw-refresh-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualPennsylvaniaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/pennsylvaniaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await expect(enqueueManualPennsylvaniaCampaignFinanceRawDataRefreshJob({ year: 2026 })).resolves.toBe("disabled");
    await expect(enqueueManualPennsylvaniaCampaignFinanceRawDataRefreshJob({ year: 2026, force: true })).resolves.toBe(
      "pa-raw-refresh-job-1"
    );
    expect(queueInstance.add).toHaveBeenCalledWith(
      "pennsylvania_campaign_finance_raw_data_refresh",
      expect.objectContaining({ year: 2026, force: true, triggeredBy: "manual" }),
      expect.any(Object)
    );
  });
});
