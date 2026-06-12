import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});
const queueAddMock = vi.fn(async () => ({ id: "job-id" }));
const queueGetJobMock = vi.fn(async () => null);
const queueCloseMock = vi.fn(async () => {});

const DEMOCRATIC_CYCLE_ID = "11111111-1111-4111-8111-111111111111";
const REPUBLICAN_CYCLE_ID = "22222222-2222-4222-8222-222222222222";

function mockRuntime(): void {
  vi.doMock("pg", () => ({
    Pool: vi.fn(() => ({
      query: poolQueryMock,
      end: poolEndMock,
    })),
  }));

  vi.doMock("bullmq", () => ({
    Queue: vi.fn(() => ({
      add: queueAddMock,
      getJob: queueGetJobMock,
      close: queueCloseMock,
    })),
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
}

describe("runPresidentialPrimaryDateResearchProducer", () => {
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    originalEnv = { ...process.env };
    vi.resetModules();
    vi.clearAllMocks();
    poolQueryMock.mockReset();
    poolEndMock.mockClear();
    queueAddMock.mockResolvedValue({ id: "job-id" });
    queueGetJobMock.mockResolvedValue(null);
    queueCloseMock.mockClear();
    process.env.PRESIDENTIAL_PRIMARY_DATES_RESEARCH_ENABLED = "true";
    process.env.PRESIDENTIAL_PRIMARY_DATES_RESEARCH_MAX_ROWS_PER_RUN = "100";
    process.env.PRESIDENTIAL_PRIMARY_DATES_RESEARCH_MAX_STATES_PER_JOB = "2";
    process.env.PRESIDENTIAL_PRIMARY_DATES_RESEARCH_MAX_JOBS_PER_RUN = "10";
    process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_QUEUE = "presidential_primary_date_research_test";
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it("returns disabled summary when the feature flag is off", async () => {
    process.env.PRESIDENTIAL_PRIMARY_DATES_RESEARCH_ENABLED = "false";

    const { runPresidentialPrimaryDateResearchProducer } = await import(
      "../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js"
    );

    const result = await runPresidentialPrimaryDateResearchProducer({
      now: new Date("2027-03-07T00:00:00.000Z"),
    });

    expect(result).toMatchObject({
      enabled: false,
      dueRowCount: 0,
      enqueuedJobCount: 0,
    });
  });

  it("rejects invalid positive integer option overrides", async () => {
    const { runPresidentialPrimaryDateResearchProducer } = await import(
      "../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js"
    );

    await expect(
      runPresidentialPrimaryDateResearchProducer({
        force: true,
        maxStatesPerJob: 0,
      })
    ).rejects.toThrow("Invalid positive integer option maxStatesPerJob: 0");
  });

  it("skips cycles before the 20-month research window opens", async () => {
    mockRuntime();
    poolQueryMock.mockResolvedValueOnce({
      rows: [{ cycle_id: DEMOCRATIC_CYCLE_ID, cycle_name: "2028 Democratic presidential primary", election_year: 2028, party: "Democratic" }],
    });

    const { runPresidentialPrimaryDateResearchProducer } = await import(
      "../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js"
    );

    const result = await runPresidentialPrimaryDateResearchProducer({
      now: new Date("2027-03-06T23:59:59.999Z"),
    });

    expect(result).toMatchObject({
      enabled: true,
      cyclesScanned: 1,
      eligibleCycleCount: 0,
      bootstrapRequestedRowCount: 0,
      dueRowCount: 0,
      enqueuedJobCount: 0,
    });
    expect(poolQueryMock).toHaveBeenCalledTimes(1);
    expect(queueAddMock).not.toHaveBeenCalled();
    expect(queueCloseMock).toHaveBeenCalledTimes(1);
  });

  it("bootstraps due cycles and enqueues grouped research jobs", async () => {
    mockRuntime();
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          { cycle_id: DEMOCRATIC_CYCLE_ID, cycle_name: "2028 Democratic presidential primary", election_year: 2028, party: "Democratic" },
          { cycle_id: REPUBLICAN_CYCLE_ID, cycle_name: "2028 Republican presidential primary", election_year: 2028, party: "Republican" },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          { id: DEMOCRATIC_CYCLE_ID, stage: "primary" },
          { id: REPUBLICAN_CYCLE_ID, stage: "primary" },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ inserted_count: "50" }] })
      .mockResolvedValueOnce({
        rows: [
          {
            cycle_id: DEMOCRATIC_CYCLE_ID,
            cycle_name: "2028 Democratic presidential primary",
            election_year: 2028,
            party: "Democratic",
            state_fips: "01",
            date_research_status: "pending",
            next_research_at: null,
          },
          {
            cycle_id: DEMOCRATIC_CYCLE_ID,
            cycle_name: "2028 Democratic presidential primary",
            election_year: 2028,
            party: "Democratic",
            state_fips: "02",
            date_research_status: "pending",
            next_research_at: null,
          },
          {
            cycle_id: DEMOCRATIC_CYCLE_ID,
            cycle_name: "2028 Democratic presidential primary",
            election_year: 2028,
            party: "Democratic",
            state_fips: "04",
            date_research_status: "not_official_yet",
            next_research_at: "2027-03-07T00:00:00.000Z",
          },
          {
            cycle_id: REPUBLICAN_CYCLE_ID,
            cycle_name: "2028 Republican presidential primary",
            election_year: 2028,
            party: "Republican",
            state_fips: "06",
            date_research_status: "error",
            next_research_at: null,
          },
        ],
      });

    const { runPresidentialPrimaryDateResearchProducer } = await import(
      "../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js"
    );

    const result = await runPresidentialPrimaryDateResearchProducer({
      now: new Date("2027-03-07T00:00:00.000Z"),
    });

    expect(result).toMatchObject({
      enabled: true,
      cyclesScanned: 2,
      eligibleCycleCount: 2,
      bootstrapRequestedRowCount: 102,
      bootstrapInsertedRowCount: 50,
      dueRowCount: 4,
      dueGroupCount: 3,
      selectedGroupCount: 3,
      enqueuedJobCount: 3,
      updatedJobCount: 0,
    });

    expect(queueAddMock).toHaveBeenCalledTimes(3);
    expect(queueAddMock).toHaveBeenCalledWith(
      "presidential_primary_date_research",
      expect.objectContaining({
        cycle_id: DEMOCRATIC_CYCLE_ID,
        election_year: 2028,
        party: "Democratic",
        state_fips_list: ["01", "02"],
        scheduled_for: "2027-03-07T00:00:00.000Z",
      }),
      expect.objectContaining({
        jobId: `presidential-primary-dates:${DEMOCRATIC_CYCLE_ID}:batch:0`,
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 300_000,
        },
      })
    );
    expect(queueAddMock).toHaveBeenCalledWith(
      "presidential_primary_date_research",
      expect.objectContaining({
        cycle_id: DEMOCRATIC_CYCLE_ID,
        state_fips_list: ["04"],
      }),
      expect.objectContaining({
        jobId: `presidential-primary-dates:${DEMOCRATIC_CYCLE_ID}:batch:1`,
      })
    );
    expect(queueAddMock).toHaveBeenCalledWith(
      "presidential_primary_date_research",
      expect.objectContaining({
        cycle_id: REPUBLICAN_CYCLE_ID,
        party: "Republican",
        state_fips_list: ["06"],
      }),
      expect.objectContaining({
        jobId: `presidential-primary-dates:${REPUBLICAN_CYCLE_ID}:batch:0`,
      })
    );
  });

  it("does not enqueue jobs in dry-run mode", async () => {
    mockRuntime();
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [{ cycle_id: DEMOCRATIC_CYCLE_ID, cycle_name: "2028 Democratic presidential primary", election_year: 2028, party: "Democratic" }],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            cycle_id: DEMOCRATIC_CYCLE_ID,
            cycle_name: "2028 Democratic presidential primary",
            election_year: 2028,
            party: "Democratic",
            state_fips: "06",
            date_research_status: "pending",
            next_research_at: null,
          },
        ],
      });

    const { runPresidentialPrimaryDateResearchProducer } = await import(
      "../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js"
    );

    const result = await runPresidentialPrimaryDateResearchProducer({
      dryRun: true,
      now: new Date("2027-03-07T00:00:00.000Z"),
    });

    expect(result).toMatchObject({
      dryRun: true,
      bootstrapRequestedRowCount: 51,
      bootstrapInsertedRowCount: 0,
      dueRowCount: 1,
      dueGroupCount: 1,
      selectedGroupCount: 1,
      enqueuedJobCount: 0,
    });
    expect(poolQueryMock).toHaveBeenCalledTimes(2);
    expect(queueAddMock).not.toHaveBeenCalled();
    expect(queueCloseMock).not.toHaveBeenCalled();
  });

  it("updates an existing non-active job by unioning state lists", async () => {
    const existingJob = {
      data: {
        state_fips_list: ["01", "02"],
      },
      getState: vi.fn(async () => "delayed"),
      remove: vi.fn(async () => {}),
    };
    queueGetJobMock.mockResolvedValue(existingJob);
    mockRuntime();
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [{ cycle_id: DEMOCRATIC_CYCLE_ID, cycle_name: "2028 Democratic presidential primary", election_year: 2028, party: "Democratic" }],
      })
      .mockResolvedValueOnce({
        rows: [{ id: DEMOCRATIC_CYCLE_ID, stage: "primary" }],
      })
      .mockResolvedValueOnce({ rows: [{ inserted_count: 0 }] })
      .mockResolvedValueOnce({
        rows: [
          {
            cycle_id: DEMOCRATIC_CYCLE_ID,
            cycle_name: "2028 Democratic presidential primary",
            election_year: 2028,
            party: "Democratic",
            state_fips: "02",
            date_research_status: "pending",
            next_research_at: null,
          },
          {
            cycle_id: DEMOCRATIC_CYCLE_ID,
            cycle_name: "2028 Democratic presidential primary",
            election_year: 2028,
            party: "Democratic",
            state_fips: "04",
            date_research_status: "pending",
            next_research_at: null,
          },
        ],
      });

    const { runPresidentialPrimaryDateResearchProducer } = await import(
      "../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js"
    );

    await expect(
      runPresidentialPrimaryDateResearchProducer({
        now: new Date("2027-03-07T00:00:00.000Z"),
      })
    ).resolves.toMatchObject({
      enqueuedJobCount: 0,
      updatedJobCount: 1,
    });

    expect(existingJob.remove).toHaveBeenCalledTimes(1);
    expect(queueAddMock).toHaveBeenCalledWith(
      "presidential_primary_date_research",
      expect.objectContaining({
        state_fips_list: ["01", "02", "04"],
      }),
      expect.objectContaining({
        jobId: `presidential-primary-dates:${DEMOCRATIC_CYCLE_ID}:batch:0`,
      })
    );
  });
});
