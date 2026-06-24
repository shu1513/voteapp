import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("michiganMitnRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED;
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
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_DAILY_CRON = "40 8 * * *";
    process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_DAILY_TZ = "America/Detroit";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMichiganMitnRawDataRefreshJobs } = await import(
      "../../src/scheduler/michiganMitnRawDataRefreshScheduler.js"
    );

    await upsertRecurringMichiganMitnRawDataRefreshJobs({ year: 2022, cacheDir: "/tmp/mi-mitn", timeoutMs: 5000 });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "michigan_mitn_raw_data_refresh_daily",
      { pattern: "40 8 * * *", tz: "America/Detroit" },
      expect.objectContaining({
        name: "michigan_mitn_raw_data_refresh",
        data: expect.objectContaining({
          year: 2022,
          url: "https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data/2022_mi_cfr.7z",
          cacheDir: "/tmp/mi-mitn",
          timeoutMs: 5000,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring job when disabled", async () => {
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMichiganMitnRawDataRefreshJobs } = await import(
      "../../src/scheduler/michiganMitnRawDataRefreshScheduler.js"
    );

    await upsertRecurringMichiganMitnRawDataRefreshJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("michigan_mitn_raw_data_refresh_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("runs a refresh job when enabled", async () => {
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshMichiganMitnLegacyArchiveCache = vi.fn().mockResolvedValue({
      status: "unchanged",
      cacheDir: "/tmp/mi-mitn",
      archivePath: "/tmp/mi-mitn/2022_mi_cfr.7z",
      metadataPath: "/tmp/mi-mitn/2022_mi_cfr.metadata.json",
      remote: { year: 2022, url: "https://example.test", contentLength: null, contentType: null, etag: null, lastModified: null },
      previous: null,
      current: {},
    });
    vi.doMock("../../src/pipeline/michiganFinance/michiganMitnLegacyArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshMichiganMitnLegacyArchiveCache,
    }));

    const { runMichiganMitnRawDataRefreshJob } = await import(
      "../../src/scheduler/michiganMitnRawDataRefreshScheduler.js"
    );

    const result = await runMichiganMitnRawDataRefreshJob({
      triggeredBy: "manual",
      year: 2022,
      cacheDir: "/tmp/mi-mitn",
      timeoutMs: 5000,
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      status: "unchanged",
    });
    expect(refreshMichiganMitnLegacyArchiveCache).toHaveBeenCalledWith(
      expect.objectContaining({ year: 2022, cacheDir: "/tmp/mi-mitn", timeoutMs: 5000 })
    );
  });

  it("does not run refresh when disabled", async () => {
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshMichiganMitnLegacyArchiveCache = vi.fn();
    vi.doMock("../../src/pipeline/michiganFinance/michiganMitnLegacyArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshMichiganMitnLegacyArchiveCache,
    }));

    const { runMichiganMitnRawDataRefreshJob } = await import(
      "../../src/scheduler/michiganMitnRawDataRefreshScheduler.js"
    );

    await expect(runMichiganMitnRawDataRefreshJob({ triggeredBy: "daily" })).resolves.toEqual(
      expect.objectContaining({
        enabled: false,
        triggeredBy: "daily",
        status: "disabled",
        refresh: null,
      })
    );
    expect(refreshMichiganMitnLegacyArchiveCache).not.toHaveBeenCalled();
  });

  it("returns disabled for manual enqueue when flags are off", async () => {
    process.env.MICHIGAN_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED = "true";
    mockEnv();

    const Queue = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualMichiganMitnRawDataRefreshJob } = await import(
      "../../src/scheduler/michiganMitnRawDataRefreshScheduler.js"
    );

    await expect(enqueueManualMichiganMitnRawDataRefreshJob()).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
  });
});
