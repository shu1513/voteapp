import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("connecticutEcrisRawDataRefreshScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED;
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
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED = "false";

    const refreshConnecticutEcrisArtifactCache = vi.fn();
    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js"
      );
      return { ...actual, refreshConnecticutEcrisArtifactCache };
    });

    const { runConnecticutEcrisRawDataRefreshJob } = await import(
      "../../src/scheduler/connecticutEcrisRawDataRefreshScheduler.js"
    );

    await expect(runConnecticutEcrisRawDataRefreshJob({ year: 2026, triggeredBy: "daily" })).resolves.toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      year: 2026,
      status: "disabled",
      refresh: null,
      independentExpenditures: null,
    });
    expect(refreshConnecticutEcrisArtifactCache).not.toHaveBeenCalled();
  });

  it("runs the refresh cache when enabled", async () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED = "true";

    const refresh = {
      status: "unchanged",
      cacheDir: "/cache",
      filePath: "/cache/2026_election_candidate_exploratory_receipts.csv",
      metadataPath: "/cache/2026_election_candidate_exploratory_receipts.metadata.json",
      remote: {
        year: 2026,
        transactionType: "receipts",
        committeeType: "candidate_exploratory",
        period: "election",
        format: "csv",
        url: "https://example.test/ct.csv",
        contentLength: 1,
        contentType: "text/csv",
        etag: "e",
        lastModified: "d",
      },
      previous: null,
      current: {
        version: 1,
        artifact: {
          year: 2026,
          transactionType: "receipts",
          committeeType: "candidate_exploratory",
          period: "election",
          format: "csv",
        },
        filePath: "/cache/2026_election_candidate_exploratory_receipts.csv",
        metadataPath: "/cache/2026_election_candidate_exploratory_receipts.metadata.json",
        downloadedAt: "2026-06-19T00:00:00.000Z",
        remote: {
          year: 2026,
          transactionType: "receipts",
          committeeType: "candidate_exploratory",
          period: "election",
          format: "csv",
          url: "https://example.test/ct.csv",
          contentLength: 1,
          contentType: "text/csv",
          etag: "e",
          lastModified: "d",
        },
        bytesWritten: 1,
      },
    };
    const refreshConnecticutEcrisArtifactCache = vi.fn().mockResolvedValue(refresh);
    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js"
      );
      return { ...actual, refreshConnecticutEcrisArtifactCache };
    });
    const fetchResult = { year: 2026, sourceUrl: "https://seec.ct.gov/ie", rows: [], searchWindows: [] };
    const fetchConnecticutEcrisIndependentExpenditures = vi.fn().mockResolvedValue(fetchResult);
    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureClient.js", () => ({
      fetchConnecticutEcrisIndependentExpenditures,
    }));
    const writeConnecticutEcrisIndependentExpenditureCache = vi.fn().mockResolvedValue({
      filePath: "/cache/2026_independent_expenditures.json",
      artifact: { version: 1, year: 2026, fetchedAt: "2026-09-01T00:00:00.000Z", rowCount: 0, rows: [] },
    });
    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureCache.js", () => ({
      writeConnecticutEcrisIndependentExpenditureCache,
    }));

    const { runConnecticutEcrisRawDataRefreshJob } = await import(
      "../../src/scheduler/connecticutEcrisRawDataRefreshScheduler.js"
    );

    const result = await runConnecticutEcrisRawDataRefreshJob({
      force: true,
      triggeredBy: "manual",
      year: 2026,
      url: "https://example.test/ct.csv",
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
      independentExpenditures: {
        status: "refreshed",
        filePath: "/cache/2026_independent_expenditures.json",
        rowCount: 0,
        fetchedAt: "2026-09-01T00:00:00.000Z",
      },
    });
    expect(fetchConnecticutEcrisIndependentExpenditures).toHaveBeenCalledWith({ year: 2026, timeoutMs: 5000 });
    expect(writeConnecticutEcrisIndependentExpenditureCache).toHaveBeenCalledWith({ cacheDir: "/cache", fetchResult });
    expect(refreshConnecticutEcrisArtifactCache).toHaveBeenCalledWith({
      year: 2026,
      transactionType: "receipts",
      committeeType: "candidate_exploratory",
      period: "election",
      format: "csv",
      cacheDir: "/cache",
      url: "https://example.test/ct.csv",
      force: true,
      timeoutMs: 5000,
    });
  });

  it("keeps the receipts refresh when the independent expenditure search fails", async () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED = "true";

    const refreshConnecticutEcrisArtifactCache = vi.fn().mockResolvedValue({ status: "downloaded" });
    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js"
      );
      return { ...actual, refreshConnecticutEcrisArtifactCache };
    });
    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureClient.js", () => ({
      fetchConnecticutEcrisIndependentExpenditures: vi.fn().mockRejectedValue(new Error("eCRIS answered 503")),
    }));
    const writeConnecticutEcrisIndependentExpenditureCache = vi.fn();
    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureCache.js", () => ({
      writeConnecticutEcrisIndependentExpenditureCache,
    }));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const { runConnecticutEcrisRawDataRefreshJob } = await import(
      "../../src/scheduler/connecticutEcrisRawDataRefreshScheduler.js"
    );

    const result = await runConnecticutEcrisRawDataRefreshJob({ triggeredBy: "daily", year: 2026, cacheDir: "/cache" });

    expect(result).toMatchObject({
      status: "downloaded",
      independentExpenditures: { status: "failed", error: "eCRIS answered 503" },
    });
    expect(writeConnecticutEcrisIndependentExpenditureCache).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalled();
  });

  it("does not search independent expenditures for a non-receipts artifact job", async () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED = "true";

    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js", async () => {
      const actual = await vi.importActual<object>(
        "../../src/pipeline/connecticutFinance/connecticutEcrisArtifactCache.js"
      );
      return { ...actual, refreshConnecticutEcrisArtifactCache: vi.fn().mockResolvedValue({ status: "unchanged" }) };
    });
    const fetchConnecticutEcrisIndependentExpenditures = vi.fn();
    vi.doMock("../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureClient.js", () => ({
      fetchConnecticutEcrisIndependentExpenditures,
    }));

    const { runConnecticutEcrisRawDataRefreshJob } = await import(
      "../../src/scheduler/connecticutEcrisRawDataRefreshScheduler.js"
    );

    const result = await runConnecticutEcrisRawDataRefreshJob({
      triggeredBy: "manual",
      year: 2026,
      transactionType: "disbursements",
      committeeType: "party_pac",
      period: "calendar",
    });

    expect(result.independentExpenditures).toBeNull();
    expect(fetchConnecticutEcrisIndependentExpenditures).not.toHaveBeenCalled();
  });

  it("upserts a daily scheduler with eCRIS defaults", async () => {
    process.env.REDIS_URL = "redis://localhost:6379/0";
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_DAILY_CRON = "20 8 * * *";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_DAILY_TZ = "America/New_York";
    mockEnv();

    const queueInstance = {
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringConnecticutEcrisRawDataRefreshJobs } = await import(
      "../../src/scheduler/connecticutEcrisRawDataRefreshScheduler.js"
    );

    await upsertRecurringConnecticutEcrisRawDataRefreshJobs({ year: 2026, cacheDir: "/cache" });

    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "connecticut_ecris_raw_data_refresh_daily",
      { pattern: "20 8 * * *", tz: "America/New_York" },
      expect.objectContaining({
        name: "connecticut_ecris_raw_data_refresh",
        data: expect.objectContaining({
          year: 2026,
          transactionType: "receipts",
          committeeType: "candidate_exploratory",
          period: "election",
          format: "csv",
          cacheDir: "/cache",
          force: false,
          triggeredBy: "daily",
        }),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("removes the daily scheduler when the master Connecticut finance flag is disabled", async () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      removeJobScheduler: vi.fn().mockResolvedValue(true),
      upsertJobScheduler: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { upsertRecurringConnecticutEcrisRawDataRefreshJobs } = await import(
      "../../src/scheduler/connecticutEcrisRawDataRefreshScheduler.js"
    );

    await upsertRecurringConnecticutEcrisRawDataRefreshJobs({ year: 2026 });

    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith("connecticut_ecris_raw_data_refresh_daily");
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("enqueues manual jobs when raw refresh is enabled or forced", async () => {
    process.env.CONNECTICUT_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED = "false";
    mockEnv();

    const queueInstance = {
      add: vi.fn().mockResolvedValue({ id: "ct-raw-refresh-job-1" }),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    vi.doMock("bullmq", () => ({ Queue, Worker: vi.fn() }));

    const { enqueueManualConnecticutEcrisRawDataRefreshJob } = await import(
      "../../src/scheduler/connecticutEcrisRawDataRefreshScheduler.js"
    );

    await expect(enqueueManualConnecticutEcrisRawDataRefreshJob({ year: 2026 })).resolves.toBe("disabled");
    await expect(enqueueManualConnecticutEcrisRawDataRefreshJob({ year: 2026, force: true })).resolves.toBe(
      "ct-raw-refresh-job-1"
    );
    expect(queueInstance.add).toHaveBeenCalledWith(
      "connecticut_ecris_raw_data_refresh",
      expect.objectContaining({ year: 2026, force: true, triggeredBy: "manual" }),
      expect.any(Object)
    );
  });
});
