import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("indianaCampaignFinanceRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED;
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
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_CRON = "15 10 * * *";
    process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_TZ = "America/Indiana/Indianapolis";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringIndianaCampaignFinanceRawDataRefreshJobs } = await import(
      "../../src/scheduler/indianaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await upsertRecurringIndianaCampaignFinanceRawDataRefreshJobs({
      year: 2026,
      artifactKind: "expenditure",
      cacheDir: "/tmp/in-campaign-finance",
      timeoutMs: 5000,
    });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(1);
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "indiana_campaign_finance_raw_data_refresh_daily",
      { pattern: "15 10 * * *", tz: "America/Indiana/Indianapolis" },
      expect.objectContaining({
        name: "indiana_campaign_finance_raw_data_refresh",
        data: expect.objectContaining({
          year: 2026,
          artifactKind: "expenditure",
          cacheDir: "/tmp/in-campaign-finance",
          timeoutMs: 5000,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the recurring job when disabled", async () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringIndianaCampaignFinanceRawDataRefreshJobs } = await import(
      "../../src/scheduler/indianaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await upsertRecurringIndianaCampaignFinanceRawDataRefreshJobs({ year: 2026 });

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "indiana_campaign_finance_raw_data_refresh_daily"
    );
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("runs a refresh job when enabled", async () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshIndianaCampaignFinanceArtifactCache = vi.fn().mockResolvedValue({
      status: "unchanged",
      cacheDir: "/tmp/in-campaign-finance",
      zipPath: "/tmp/in-campaign-finance/2026_ContributionData.csv.zip",
      metadataPath: "/tmp/in-campaign-finance/2026_contribution.metadata.json",
      remote: {
        year: 2026,
        artifactKind: "contribution",
        url: "https://example.test/2026_ContributionData.csv.zip",
        contentLength: null,
        contentType: null,
        etag: null,
        lastModified: null,
      },
      previous: null,
      current: {},
    });
    vi.doMock("../../src/pipeline/indianaFinance/indianaCampaignFinanceArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshIndianaCampaignFinanceArtifactCache,
    }));

    const { runIndianaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/indianaCampaignFinanceRawDataRefreshScheduler.js"
    );

    const result = await runIndianaCampaignFinanceRawDataRefreshJob({
      triggeredBy: "manual",
      year: 2026,
      artifactKind: "contribution",
      cacheDir: "/tmp/in-campaign-finance",
      timeoutMs: 5000,
    });

    expect(result).toMatchObject({
      enabled: true,
      force: false,
      triggeredBy: "manual",
      status: "unchanged",
    });
    expect(refreshIndianaCampaignFinanceArtifactCache).toHaveBeenCalledWith(
      expect.objectContaining({
        year: 2026,
        artifactKind: "contribution",
        cacheDir: "/tmp/in-campaign-finance",
        timeoutMs: 5000,
      })
    );
  });

  it("does not run refresh when disabled", async () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "true";
    const refreshIndianaCampaignFinanceArtifactCache = vi.fn();
    vi.doMock("../../src/pipeline/indianaFinance/indianaCampaignFinanceArtifactCache.js", async (importOriginal) => ({
      ...(await importOriginal<object>()),
      refreshIndianaCampaignFinanceArtifactCache,
    }));

    const { runIndianaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/indianaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await expect(runIndianaCampaignFinanceRawDataRefreshJob({ triggeredBy: "daily", year: 2026 })).resolves.toEqual(
      expect.objectContaining({
        enabled: false,
        triggeredBy: "daily",
        status: "disabled",
        refresh: null,
      })
    );
    expect(refreshIndianaCampaignFinanceArtifactCache).not.toHaveBeenCalled();
  });

  it("returns disabled for manual enqueue when flags are off", async () => {
    process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED = "true";
    mockEnv();

    const Queue = vi.fn();
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualIndianaCampaignFinanceRawDataRefreshJob } = await import(
      "../../src/scheduler/indianaCampaignFinanceRawDataRefreshScheduler.js"
    );

    await expect(enqueueManualIndianaCampaignFinanceRawDataRefreshJob({ year: 2026 })).resolves.toBe("disabled");
    expect(Queue).not.toHaveBeenCalled();
  });
});
