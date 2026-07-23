import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("coloradoTracerRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED;
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
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED = "false";

    const refreshColoradoTracerContributionArtifactCache = vi.fn();
    vi.doMock("../../src/pipeline/coloradoFinance/coloradoTracerContributionArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/coloradoFinance/coloradoTracerContributionArtifactCache.js"
      );
      return { ...actual, refreshColoradoTracerContributionArtifactCache };
    });

    const { runColoradoTracerRawDataRefreshJob } = await import(
      "../../src/scheduler/coloradoTracerRawDataRefreshScheduler.js"
    );

    await expect(runColoradoTracerRawDataRefreshJob({ year: 2026, triggeredBy: "daily" })).resolves.toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      year: 2026,
      status: "disabled",
      refresh: null,
    });
    expect(refreshColoradoTracerContributionArtifactCache).not.toHaveBeenCalled();
  });

  it("runs the refresh cache when enabled", async () => {
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED = "true";

    const refresh = {
      status: "unchanged",
      cacheDir: "/cache",
      zipPath: "/cache/2026_ContributionData.csv.zip",
      metadataPath: "/cache/2026_ContributionData.metadata.json",
      remote: {
        year: 2026,
        url: "https://example.test/2026.zip",
        contentLength: 1,
        contentType: "zip",
        etag: "e",
        lastModified: "d",
      },
      previous: null,
      current: {
        version: 1,
        year: 2026,
        zipPath: "/cache/2026_ContributionData.csv.zip",
        metadataPath: "/cache/2026_ContributionData.metadata.json",
        downloadedAt: "2026-06-19T00:00:00.000Z",
        remote: {
          year: 2026,
          url: "https://example.test/2026.zip",
          contentLength: 1,
          contentType: "zip",
          etag: "e",
          lastModified: "d",
        },
        bytesWritten: 1,
      },
    };
    const refreshColoradoTracerContributionArtifactCache = vi.fn().mockResolvedValue(refresh);
    vi.doMock("../../src/pipeline/coloradoFinance/coloradoTracerContributionArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/coloradoFinance/coloradoTracerContributionArtifactCache.js"
      );
      return { ...actual, refreshColoradoTracerContributionArtifactCache };
    });

    const { runColoradoTracerRawDataRefreshJob } = await import(
      "../../src/scheduler/coloradoTracerRawDataRefreshScheduler.js"
    );

    const result = await runColoradoTracerRawDataRefreshJob({
      force: true,
      triggeredBy: "manual",
      year: 2026,
      url: "https://example.test/2026.zip",
      cacheDir: "/cache",
      timeoutMs: 5000,
    });

    expect(result).toEqual({
      enabled: true,
      force: true,
      triggeredBy: "manual",
      year: 2026,
      status: "unchanged",
      refresh,
    });
    expect(refreshColoradoTracerContributionArtifactCache).toHaveBeenCalledWith({
      year: 2026,
      cacheDir: "/cache",
      url: "https://example.test/2026.zip",
      force: true,
      timeoutMs: 5000,
    });
  });

  it("upserts a daily scheduler with metadata-check defaults", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.COLORADO_TRACER_RAW_DATA_REFRESH_DAILY_CRON = "20 8 * * *";
    process.env.COLORADO_TRACER_RAW_DATA_REFRESH_DAILY_TZ = "America/Denver";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringColoradoTracerRawDataRefreshJobs } = await import(
      "../../src/scheduler/coloradoTracerRawDataRefreshScheduler.js"
    );

    await upsertRecurringColoradoTracerRawDataRefreshJobs({ year: 2026, cacheDir: "/cache" });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "colorado_tracer_raw_data_refresh_daily",
      { pattern: "20 8 * * *", tz: "America/Denver" },
      expect.objectContaining({
        name: "colorado_tracer_raw_data_refresh",
        data: expect.objectContaining({
          year: 2026,
          cacheDir: "/cache",
          force: false,
          timeoutMs: 900_000,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the daily scheduler when the master Colorado finance flag is disabled", async () => {
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringColoradoTracerRawDataRefreshJobs } = await import(
      "../../src/scheduler/coloradoTracerRawDataRefreshScheduler.js"
    );

    await upsertRecurringColoradoTracerRawDataRefreshJobs({ year: 2026 });

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("colorado_tracer_raw_data_refresh_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs when raw refresh is enabled or forced", async () => {
    process.env.COLORADO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "co-raw-refresh-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualColoradoTracerRawDataRefreshJob } = await import(
      "../../src/scheduler/coloradoTracerRawDataRefreshScheduler.js"
    );

    await expect(enqueueManualColoradoTracerRawDataRefreshJob({ year: 2026 })).resolves.toBe("disabled");
    await expect(enqueueManualColoradoTracerRawDataRefreshJob({ year: 2026, force: true })).resolves.toBe(
      "co-raw-refresh-job-1"
    );
    expect(queueInstance.add).toHaveBeenCalledWith(
      "colorado_tracer_raw_data_refresh",
      expect.objectContaining({ year: 2026, force: true, triggeredBy: "manual" }),
      expect.any(Object)
    );
  });
});
