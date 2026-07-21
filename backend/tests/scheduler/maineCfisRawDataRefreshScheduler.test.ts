import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("maineCfisRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.MAINE_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED;
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
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_DAILY_CRON = "15 8 * * *";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_DAILY_TZ = "America/New_York";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMaineCfisRawDataRefreshJobs } = await import(
      "../../src/scheduler/maineCfisRawDataRefreshScheduler.js"
    );

    await upsertRecurringMaineCfisRawDataRefreshJobs({ filingYear: 2026, cacheDir: "/tmp/maine-cfis" });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(2);
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "maine_cfis_raw_data_refresh_contributions_daily",
      { pattern: "15 8 * * *", tz: "America/New_York" },
      expect.objectContaining({
        name: "maine_cfis_raw_data_refresh",
        data: expect.objectContaining({
          artifactKind: "contributions",
          filingYear: 2026,
          cacheDir: "/tmp/maine-cfis",
        }),
      })
    );
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "maine_cfis_raw_data_refresh_expenditures_daily",
      { pattern: "15 8 * * *", tz: "America/New_York" },
      expect.objectContaining({
        name: "maine_cfis_raw_data_refresh",
        data: expect.objectContaining({
          artifactKind: "expenditures",
          filingYear: 2026,
          cacheDir: "/tmp/maine-cfis",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the unselected recurring job when upserting one artifact kind", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMaineCfisRawDataRefreshJobs } = await import(
      "../../src/scheduler/maineCfisRawDataRefreshScheduler.js"
    );

    await upsertRecurringMaineCfisRawDataRefreshJobs({ filingYear: 2026, artifactKind: "expenditures" });

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("maine_cfis_raw_data_refresh_contributions_daily");
    expect(queueInstance.removeJobScheduler).not.toHaveBeenCalledWith("maine_cfis_raw_data_refresh_expenditures_daily");
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(1);
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "maine_cfis_raw_data_refresh_expenditures_daily",
      expect.any(Object),
      expect.objectContaining({
        data: expect.objectContaining({ artifactKind: "expenditures", filingYear: 2026 }),
      })
    );
  });

  it("removes both recurring jobs when disabled", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMaineCfisRawDataRefreshJobs } = await import(
      "../../src/scheduler/maineCfisRawDataRefreshScheduler.js"
    );

    await upsertRecurringMaineCfisRawDataRefreshJobs();

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("maine_cfis_raw_data_refresh_contributions_daily");
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("maine_cfis_raw_data_refresh_expenditures_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("returns a disabled no-op result when the raw refresh flag is off", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED = "false";
    const refreshMaineCfisArtifactCache = vi.fn();
    vi.doMock("../../src/pipeline/maineFinance/maineCfisArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshMaineCfisArtifactCache,
    }));

    const { runMaineCfisRawDataRefreshJob } = await import(
      "../../src/scheduler/maineCfisRawDataRefreshScheduler.js"
    );

    const result = await runMaineCfisRawDataRefreshJob({
      filingYear: 2026,
      artifactKind: "expenditures",
      triggeredBy: "daily",
    });

    expect(result).toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      filingYears: [2026],
      artifactKind: "expenditures",
      status: "disabled",
      refreshes: [],
    });
    expect(refreshMaineCfisArtifactCache).not.toHaveBeenCalled();
  });

  it("runs a refresh job when enabled", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshMaineCfisArtifactCache = vi.fn().mockResolvedValue({
      status: "unchanged",
      cacheDir: "/tmp/maine-cfis",
      filePath: "/tmp/maine-cfis/CON_2026.csv",
      metadataPath: "/tmp/maine-cfis/CON_2026.metadata.json",
      remote: {
        filingYear: 2026,
        artifactKind: "contributions",
        url: "https://mainecampaignfinance.com/api/DataDownload/CSVDownloadReport",
        requestBody: { year: 2026, transactionType: "CON" },
        contentLength: null,
        contentType: null,
        contentDisposition: null,
        etag: null,
        lastModified: null,
      },
      previous: null,
      current: {},
    });
    vi.doMock("../../src/pipeline/maineFinance/maineCfisArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshMaineCfisArtifactCache,
    }));

    const { runMaineCfisRawDataRefreshJob } = await import(
      "../../src/scheduler/maineCfisRawDataRefreshScheduler.js"
    );

    const result = await runMaineCfisRawDataRefreshJob({
      filingYear: 2026,
      artifactKind: "contributions",
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: true,
      filingYears: [2026],
      artifactKind: "contributions",
      status: "unchanged",
    });
    expect(result.refreshes).toHaveLength(1);
    expect(refreshMaineCfisArtifactCache).toHaveBeenCalledTimes(1);
    expect(refreshMaineCfisArtifactCache).toHaveBeenCalledWith(
      expect.objectContaining({ filingYear: 2026, artifactKind: "contributions" })
    );
  });

  it("refreshes both cycle filing years when no filingYear is pinned", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshMaineCfisArtifactCache = vi
      .fn()
      .mockImplementation(async ({ filingYear }: { filingYear: number }) => ({
        status: filingYear % 2 === 0 ? "downloaded" : "unchanged",
      }));
    vi.doMock("../../src/pipeline/maineFinance/maineCfisArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshMaineCfisArtifactCache,
    }));

    const { runMaineCfisRawDataRefreshJob } = await import(
      "../../src/scheduler/maineCfisRawDataRefreshScheduler.js"
    );

    const result = await runMaineCfisRawDataRefreshJob({ artifactKind: "contributions", triggeredBy: "daily" });

    const currentYear = new Date().getUTCFullYear();
    expect(result.filingYears).toEqual([currentYear - 1, currentYear]);
    expect(result.refreshes.map((outcome) => outcome.filingYear)).toEqual([currentYear - 1, currentYear]);
    expect(result.status).toBe("downloaded");
    expect(refreshMaineCfisArtifactCache).toHaveBeenCalledTimes(2);
    expect(refreshMaineCfisArtifactCache).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ filingYear: currentYear - 1, artifactKind: "contributions" })
    );
    expect(refreshMaineCfisArtifactCache).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ filingYear: currentYear, artifactKind: "contributions" })
    );
  });

  it("omits filingYear from recurring job data when not pinned", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringMaineCfisRawDataRefreshJobs } = await import(
      "../../src/scheduler/maineCfisRawDataRefreshScheduler.js"
    );

    await upsertRecurringMaineCfisRawDataRefreshJobs();

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(2);
    for (const call of queueInstance.upsertJobScheduler.mock.calls) {
      const data = (call[2] as { data: Record<string, unknown> }).data;
      expect(data).not.toHaveProperty("filingYear");
    }
  });

  it("enqueues a manual refresh job when enabled", async () => {
    process.env.MAINE_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.MAINE_CFIS_RAW_DATA_REFRESH_ENABLED = "true";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "job-123" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualMaineCfisRawDataRefreshJob } = await import(
      "../../src/scheduler/maineCfisRawDataRefreshScheduler.js"
    );

    const jobId = await enqueueManualMaineCfisRawDataRefreshJob({
      filingYear: 2026,
      artifactKind: "expenditures",
      cacheDir: "/tmp/maine-cfis",
    });

    expect(jobId).toBe("job-123");
    expect(queueInstance.add).toHaveBeenCalledWith(
      "maine_cfis_raw_data_refresh",
      expect.objectContaining({
        filingYear: 2026,
        artifactKind: "expenditures",
        cacheDir: "/tmp/maine-cfis",
        triggeredBy: "manual",
      }),
      expect.objectContaining({ removeOnComplete: 1000, removeOnFail: 1000 })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
