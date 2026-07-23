import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("newMexicoCfisRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED;
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

  it("upserts contribution and expenditure recurring jobs", async () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_DAILY_CRON = "15 8 * * *";
    process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_DAILY_TZ = "America/Denver";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringNewMexicoCfisRawDataRefreshJobs } = await import(
      "../../src/scheduler/newMexicoCfisRawDataRefreshScheduler.js"
    );

    await upsertRecurringNewMexicoCfisRawDataRefreshJobs({ year: 2026, cacheDir: "/tmp/cfis" });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(2);
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "new_mexico_cfis_raw_data_refresh_contributions_daily",
      { pattern: "15 8 * * *", tz: "America/Denver" },
      expect.objectContaining({
        name: "new_mexico_cfis_raw_data_refresh",
        data: expect.objectContaining({
          artifactKind: "contributions",
          year: 2026,
          cacheDir: "/tmp/cfis",
          timeoutMs: 900_000,
        }),
      })
    );
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "new_mexico_cfis_raw_data_refresh_expenditures_daily",
      { pattern: "15 8 * * *", tz: "America/Denver" },
      expect.objectContaining({
        name: "new_mexico_cfis_raw_data_refresh",
        data: expect.objectContaining({
          artifactKind: "expenditures",
          year: 2026,
          cacheDir: "/tmp/cfis",
          timeoutMs: 900_000,
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes both recurring jobs when disabled", async () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringNewMexicoCfisRawDataRefreshJobs } = await import(
      "../../src/scheduler/newMexicoCfisRawDataRefreshScheduler.js"
    );

    await upsertRecurringNewMexicoCfisRawDataRefreshJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "new_mexico_cfis_raw_data_refresh_contributions_daily"
    );
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "new_mexico_cfis_raw_data_refresh_expenditures_daily"
    );
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("runs a refresh job when enabled", async () => {
    process.env.NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshNewMexicoCfisArtifactCache = vi.fn().mockResolvedValue({
      status: "unchanged",
      cacheDir: "/tmp/cfis",
      filePath: "/tmp/cfis/CON_2026.csv",
      metadataPath: "/tmp/cfis/CON_2026.metadata.json",
      remote: { year: 2026, artifactKind: "contributions", url: "https://example.test", contentLength: null, contentType: null, etag: null, lastModified: null },
      previous: null,
      current: {},
    });
    vi.doMock("../../src/pipeline/newMexicoFinance/newMexicoCfisArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshNewMexicoCfisArtifactCache,
    }));

    const { runNewMexicoCfisRawDataRefreshJob } = await import(
      "../../src/scheduler/newMexicoCfisRawDataRefreshScheduler.js"
    );

    const result = await runNewMexicoCfisRawDataRefreshJob({
      year: 2026,
      artifactKind: "contributions",
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      year: 2026,
      artifactKind: "contributions",
      status: "unchanged",
    });
    expect(refreshNewMexicoCfisArtifactCache).toHaveBeenCalledWith(
      expect.objectContaining({ year: 2026, artifactKind: "contributions" })
    );
  });
});
