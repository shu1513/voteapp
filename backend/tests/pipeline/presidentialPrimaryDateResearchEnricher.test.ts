import { beforeEach, describe, expect, it, vi } from "vitest";
import type { PresidentialPrimaryDateResearchJobData } from "../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js";

const CYCLE_ID = "00000000-0000-4000-8000-000000000001";
const RESEARCHED_AT = new Date("2027-03-07T00:00:00.000Z");

const enrichMock = vi.hoisted(() => vi.fn());
const workerMock = vi.hoisted(() => vi.fn());

const poolQueryMock = vi.fn();
const clientQueryMock = vi.fn();
const clientReleaseMock = vi.fn();
const poolEndMock = vi.fn(async () => {});

function makePool() {
  return {
    query: poolQueryMock,
    connect: vi.fn(async () => ({
      query: clientQueryMock,
      release: clientReleaseMock,
    })),
    end: poolEndMock,
  };
}

function makeJob(
  overrides: Partial<PresidentialPrimaryDateResearchJobData> = {}
): PresidentialPrimaryDateResearchJobData {
  return {
    cycle_id: CYCLE_ID,
    election_year: 2028,
    party: "Democratic",
    state_fips_list: ["06"],
    scheduled_for: "2027-03-07T00:00:00.000Z",
    run_id: "run-1",
    ...overrides,
  };
}

describe("processPresidentialPrimaryDateResearchJob", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
    poolQueryMock.mockResolvedValue({
      rowCount: 1,
      rows: [{ state_fips: "06", cycle_name: "2028 Democratic presidential primary" }],
    });
    clientQueryMock.mockImplementation(async () => ({ rowCount: 1, rows: [] }));

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

    vi.doMock("../../src/ai/enrichPresidentialPrimaryDates.js", () => ({
      buildPresidentialPrimaryDateAiConfigFromEnv: () => ({ timeoutMs: 90_000 }),
      enrichPresidentialPrimaryDates: enrichMock,
    }));
  });

  it("writes successful primary-date research rows in a transaction", async () => {
    enrichMock.mockResolvedValueOnce({
      ok: true,
      provider: "claude",
      model: "claude-sonnet-4-6",
      payload: {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://www.sos.ca.gov/elections/calendar"],
            notes: "Official calendar lists the date.",
          },
        ],
      },
      failedRows: [],
      sourceVerifications: [],
      aiRawDebug: null,
    });

    const { processPresidentialPrimaryDateResearchJob } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );
    const result = await processPresidentialPrimaryDateResearchJob(makeJob(), {
      pool: makePool() as never,
      researchedAt: RESEARCHED_AT,
    });

    expect(result).toMatchObject({
      cycle_id: CYCLE_ID,
      requested_state_count: 1,
      skipped_state_count: 0,
      official_found_count: 1,
      not_official_yet_count: 0,
      error_count: 0,
      rows_updated: 1,
      next_research_at: null,
      provider: "claude",
      model: "claude-sonnet-4-6",
    });
    expect(clientQueryMock).toHaveBeenCalledWith("BEGIN");
    expect(clientQueryMock).toHaveBeenCalledWith("COMMIT");
    expect(clientReleaseMock).toHaveBeenCalledTimes(1);
    expect(poolEndMock).not.toHaveBeenCalled();
  });

  it("marks rows error and completes the job when AI research fails", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rowCount: 2,
      rows: [{ state_fips: "06", cycle_name: "2028 Democratic presidential primary" }, { state_fips: "12", cycle_name: "2028 Democratic presidential primary" }],
    });
    clientQueryMock.mockImplementation(async (sql: string) => {
      if (String(sql).includes("date_research_status = 'error'")) {
        return { rowCount: 2, rows: [] };
      }
      return { rowCount: 1, rows: [] };
    });
    enrichMock.mockResolvedValueOnce({
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "results missing state_fips: 12",
    });

    const { processPresidentialPrimaryDateResearchJob } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );
    const result = await processPresidentialPrimaryDateResearchJob(
      makeJob({ state_fips_list: ["06", "12"] }),
      {
        pool: makePool() as never,
        researchedAt: RESEARCHED_AT,
      }
    );

    expect(result).toMatchObject({
      requested_state_count: 2,
      skipped_state_count: 0,
      official_found_count: 0,
      not_official_yet_count: 0,
      error_count: 2,
      rows_updated: 2,
      next_research_at: "2027-04-07T00:00:00.000Z",
      error: "results missing state_fips: 12",
    });
    expect(clientQueryMock).toHaveBeenCalledWith("BEGIN");
    expect(clientQueryMock).toHaveBeenCalledWith("COMMIT");
    expect(clientQueryMock).not.toHaveBeenCalledWith("ROLLBACK");
  });

  it("writes valid rows and marks only partial failed rows as retryable errors", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rowCount: 2,
      rows: [{ state_fips: "06", cycle_name: "2028 Democratic presidential primary" }, { state_fips: "12", cycle_name: "2028 Democratic presidential primary" }],
    });
    clientQueryMock.mockImplementation(async (sql: string) => {
      if (String(sql).includes("date_research_status = 'error'")) {
        return { rowCount: 1, rows: [] };
      }
      return { rowCount: 1, rows: [] };
    });
    enrichMock.mockResolvedValueOnce({
      ok: true,
      provider: "claude",
      model: "claude-sonnet-4-6",
      payload: {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://www.sos.ca.gov/elections/calendar"],
            notes: "Official calendar lists the date.",
          },
        ],
      },
      failedRows: [
        {
          state_fips: "12",
          reason: "presidential primary date source URL is not reachable",
        },
      ],
      sourceVerifications: [],
      aiRawDebug: null,
    });

    const { processPresidentialPrimaryDateResearchJob } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );
    const result = await processPresidentialPrimaryDateResearchJob(
      makeJob({ state_fips_list: ["06", "12"] }),
      {
        pool: makePool() as never,
        researchedAt: RESEARCHED_AT,
      }
    );

    expect(result).toMatchObject({
      requested_state_count: 2,
      skipped_state_count: 0,
      official_found_count: 1,
      not_official_yet_count: 0,
      error_count: 1,
      rows_updated: 2,
      next_research_at: "2027-04-07T00:00:00.000Z",
      provider: "claude",
      model: "claude-sonnet-4-6",
    });
    const errorUpdate = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("date_research_status = 'error'")
    );
    expect(errorUpdate?.[1]).toEqual([
      CYCLE_ID,
      ["12"],
      RESEARCHED_AT.toISOString(),
      "2027-04-07T00:00:00.000Z",
      "Partial presidential primary date research failure: 12: presidential primary date source URL is not reachable",
    ]);
    expect(clientQueryMock).toHaveBeenCalledWith("COMMIT");
  });

  it("skips stale states that are no longer due before calling AI", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rowCount: 1,
      rows: [{ state_fips: "06", cycle_name: "2028 Democratic presidential primary" }],
    });
    enrichMock.mockResolvedValueOnce({
      ok: true,
      provider: "claude",
      model: "claude-sonnet-4-6",
      payload: {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://www.sos.ca.gov/elections/calendar"],
            notes: "Official calendar lists the date.",
          },
        ],
      },
      failedRows: [],
      sourceVerifications: [],
      aiRawDebug: null,
    });

    const { processPresidentialPrimaryDateResearchJob } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );
    const result = await processPresidentialPrimaryDateResearchJob(
      makeJob({ state_fips_list: ["06", "12"] }),
      {
        pool: makePool() as never,
        researchedAt: RESEARCHED_AT,
      }
    );

    expect(enrichMock).toHaveBeenCalledWith(
      expect.objectContaining({
        stateFipsList: ["06"],
      }),
      expect.any(Object),
      undefined
    );
    expect(result).toMatchObject({
      requested_state_count: 2,
      skipped_state_count: 1,
      official_found_count: 1,
      rows_updated: 1,
    });
  });

  it("completes without AI when every queued state is stale or not due", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rowCount: 0,
      rows: [],
    });

    const { processPresidentialPrimaryDateResearchJob } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );
    const result = await processPresidentialPrimaryDateResearchJob(
      makeJob({ state_fips_list: ["06", "12"] }),
      {
        pool: makePool() as never,
        researchedAt: RESEARCHED_AT,
      }
    );

    expect(enrichMock).not.toHaveBeenCalled();
    expect(clientQueryMock).not.toHaveBeenCalled();
    expect(result).toEqual({
      cycle_id: CYCLE_ID,
      election_year: 2028,
      party: "Democratic",
      requested_state_count: 2,
      skipped_state_count: 2,
      official_found_count: 0,
      not_official_yet_count: 0,
      error_count: 0,
      rows_updated: 0,
      next_research_at: null,
    });
  });

  it("returns a disabled result without DB or AI work when presidential elections are disabled", async () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";

    const { processPresidentialPrimaryDateResearchJob } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );
    const result = await processPresidentialPrimaryDateResearchJob(
      makeJob({ state_fips_list: ["06", "12"] }),
      {
        pool: makePool() as never,
        researchedAt: RESEARCHED_AT,
      }
    );

    expect(result).toEqual({
      cycle_id: CYCLE_ID,
      election_year: 2028,
      party: "Democratic",
      requested_state_count: 2,
      skipped_state_count: 2,
      official_found_count: 0,
      not_official_yet_count: 0,
      error_count: 0,
      rows_updated: 0,
      next_research_at: null,
      disabled: true,
    });
    expect(poolQueryMock).not.toHaveBeenCalled();
    expect(clientQueryMock).not.toHaveBeenCalled();
    expect(enrichMock).not.toHaveBeenCalled();
  });
});

describe("createPresidentialPrimaryDateResearchWorker", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    workerMock.mockReset();

    vi.doMock("bullmq", () => ({
      Worker: workerMock,
    }));
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
  });

  it("passes configured concurrency to the BullMQ worker", async () => {
    const { createPresidentialPrimaryDateResearchWorker } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );

    createPresidentialPrimaryDateResearchWorker(3);

    expect(workerMock).toHaveBeenCalledWith(
      "presidential_primary_date_research",
      expect.any(Function),
      expect.objectContaining({
        concurrency: 3,
      })
    );
  });

  it("rejects invalid worker concurrency", async () => {
    const { createPresidentialPrimaryDateResearchWorker } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );

    expect(() => createPresidentialPrimaryDateResearchWorker(0)).toThrow(
      "Invalid presidential primary date research worker concurrency"
    );
  });

  it("once mode waits for drained instead of closing after the first completed job", async () => {
    const handlers = new Map<string, (...args: never[]) => void>();
    const close = vi.fn(async () => {});
    workerMock.mockImplementation(
      () =>
        ({
          once: vi.fn((event: string, handler: (...args: never[]) => void) => {
            handlers.set(event, handler);
            return undefined;
          }),
          close,
        }) as never
    );

    const { runPresidentialPrimaryDateResearchEnricher } = await import(
      "../../src/pipeline/enrichers/presidentialPrimaryDateResearchEnricher.js"
    );

    const run = runPresidentialPrimaryDateResearchEnricher({
      once: true,
      blockMs: 10_000,
      concurrency: 2,
    });

    handlers.get("completed")?.();
    await Promise.resolve();
    expect(close).not.toHaveBeenCalled();

    handlers.get("drained")?.();
    await expect(run).resolves.toBeUndefined();
    expect(close).toHaveBeenCalledTimes(1);
  });
});
