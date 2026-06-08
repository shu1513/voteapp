import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});
const queueAddMock = vi.fn(async () => ({ id: "job-id" }));
const queueGetJobMock = vi.fn(async () => null);
const queueCloseMock = vi.fn(async () => {});
const redisConnectMock = vi.fn(async () => {});
const redisExistsMock = vi.fn(async () => 0);
const redisSetMock = vi.fn(async () => "OK");
const redisQuitMock = vi.fn(async () => {});

describe("runElectionResultScheduleProducer", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    poolQueryMock.mockReset();
    queueAddMock.mockResolvedValue({ id: "job-id" });
    queueGetJobMock.mockResolvedValue(null);
    redisExistsMock.mockResolvedValue(0);
    redisSetMock.mockResolvedValue("OK");
    process.env.ELECTION_RESULTS_SCHEDULER_ENABLED = "true";
    process.env.ELECTION_RESULTS_LOOKAHEAD_HOURS = "6";
    process.env.ELECTION_RESULTS_MAX_GROUPS_PER_RUN = "10";
    process.env.ELECTION_RESULT_SEARCH_QUEUE = "election_result_search_test";
  });

  it("returns disabled summary when feature flag is off", async () => {
    process.env.ELECTION_RESULTS_SCHEDULER_ENABLED = "false";

    const { runElectionResultScheduleProducer } = await import(
      "../../src/pipeline/producers/electionResultScheduleProducer.js"
    );

    const result = await runElectionResultScheduleProducer({ now: new Date("2026-06-03T00:00:00.000Z") });

    expect(result.enabled).toBe(false);
    expect(result.dueGroupCount).toBe(0);
    expect(result.enqueuedJobCount).toBe(0);
  });

  it("groups due elections by state/date/pass and enqueues delayed search jobs", async () => {
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

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        exists: redisExistsMock,
        set: redisSetMock,
        quit: redisQuitMock,
        isOpen: true,
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

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "e-ca-1",
            state: "CA",
            election_date: "2026-06-02",
            election_night_results_checked_at: null,
            certified_results_checked_at: "2026-06-01T00:00:00.000Z",
            election_night_results_attempt_count: 0,
          },
          {
            election_id: "e-ca-2",
            state: "CA",
            election_date: "2026-06-02",
            election_night_results_checked_at: null,
            certified_results_checked_at: "2026-06-01T00:00:00.000Z",
            election_night_results_attempt_count: 0,
          },
          {
            election_id: "e-ny-1",
            state: "NY",
            election_date: "2026-06-02",
            election_night_results_checked_at: null,
            certified_results_checked_at: "2026-06-01T00:00:00.000Z",
            election_night_results_attempt_count: 0,
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ scheduled_at: new Date("2026-06-03T03:10:00.000Z") }] })
      .mockResolvedValueOnce({ rows: [{ scheduled_at: new Date("2026-06-03T01:10:00.000Z") }] });

    const { runElectionResultScheduleProducer } = await import(
      "../../src/pipeline/producers/electionResultScheduleProducer.js"
    );

    const result = await runElectionResultScheduleProducer({ now: new Date("2026-06-03T00:00:00.000Z") });

    expect(result).toMatchObject({
      enabled: true,
      dueElectionCount: 3,
      dueGroupCount: 2,
      selectedGroupCount: 2,
      enqueuedJobCount: 2,
      updatedJobCount: 0,
      markerSkippedElectionCount: 0,
    });

    expect(queueAddMock).toHaveBeenCalledTimes(2);
    expect(redisExistsMock).toHaveBeenCalledTimes(3);
    expect(redisSetMock).toHaveBeenCalledTimes(3);
    expect(redisSetMock).toHaveBeenCalledWith(
      "staging:election_result_emitted:election_night:e-ca-1",
      "2026-06-03T00:00:00.000Z",
      { EX: 54_600 }
    );
    expect(queueAddMock).toHaveBeenCalledWith(
      "election_result_search",
      expect.objectContaining({
        state: "NY",
        election_date: "2026-06-02",
        pass_type: "election_night",
        scheduled_for: "2026-06-03T01:10:00.000Z",
        election_ids: ["e-ny-1"],
      }),
      expect.objectContaining({
        jobId: "election-results:NY:2026-06-02:election_night",
        delay: 4_200_000,
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 300_000,
        },
      })
    );
    expect(queueAddMock).toHaveBeenCalledWith(
      "election_result_search",
      expect.objectContaining({
        state: "CA",
        election_ids: ["e-ca-1", "e-ca-2"],
      }),
      expect.objectContaining({
        jobId: "election-results:CA:2026-06-02:election_night",
        delay: 11_400_000,
      })
    );
  });

  it("updates an existing non-active delayed search job", async () => {
    const existingJob = {
      data: undefined,
      getState: vi.fn(async () => "delayed"),
      remove: vi.fn(async () => {}),
    };
    queueGetJobMock.mockResolvedValue(existingJob);

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

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        exists: redisExistsMock,
        set: redisSetMock,
        quit: redisQuitMock,
        isOpen: true,
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

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "e-ca-1",
            state: "CA",
            election_date: "2026-06-02",
            election_night_results_checked_at: null,
            certified_results_checked_at: "2026-06-01T00:00:00.000Z",
            election_night_results_attempt_count: 0,
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ scheduled_at: new Date("2026-06-03T03:10:00.000Z") }] });

    const { runElectionResultScheduleProducer } = await import(
      "../../src/pipeline/producers/electionResultScheduleProducer.js"
    );

    const result = await runElectionResultScheduleProducer({ now: new Date("2026-06-03T00:00:00.000Z") });

    expect(existingJob.remove).toHaveBeenCalledTimes(1);
    expect(result.updatedJobCount).toBe(1);
    expect(result.enqueuedJobCount).toBe(0);
    expect(redisSetMock).toHaveBeenCalledTimes(1);
  });

  it("preserves already-emitted election ids when a late election updates a delayed group job", async () => {
    const existingJob = {
      data: {
        state: "CA",
        election_date: "2026-06-02",
        pass_type: "election_night",
        scheduled_for: "2026-06-03T03:10:00.000Z",
        election_ids: ["e-ca-1", "e-ca-2"],
        run_id: "previous-run",
      },
      getState: vi.fn(async () => "delayed"),
      remove: vi.fn(async () => {}),
    };
    queueGetJobMock.mockResolvedValue(existingJob);

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

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        exists: redisExistsMock,
        set: redisSetMock,
        quit: redisQuitMock,
        isOpen: true,
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

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "e-ca-3",
            state: "CA",
            election_date: "2026-06-02",
            election_night_results_checked_at: null,
            certified_results_checked_at: "2026-06-01T00:00:00.000Z",
            election_night_results_attempt_count: 0,
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ scheduled_at: new Date("2026-06-03T03:10:00.000Z") }] });

    const { runElectionResultScheduleProducer } = await import(
      "../../src/pipeline/producers/electionResultScheduleProducer.js"
    );

    const result = await runElectionResultScheduleProducer({ now: new Date("2026-06-03T00:00:00.000Z") });

    expect(result.updatedJobCount).toBe(1);
    expect(existingJob.remove).toHaveBeenCalledTimes(1);
    expect(queueAddMock).toHaveBeenCalledWith(
      "election_result_search",
      expect.objectContaining({
        state: "CA",
        election_date: "2026-06-02",
        pass_type: "election_night",
        election_ids: ["e-ca-1", "e-ca-2", "e-ca-3"],
      }),
      expect.objectContaining({
        jobId: "election-results:CA:2026-06-02:election_night",
      })
    );
    expect(redisSetMock).toHaveBeenCalledTimes(1);
    expect(redisSetMock).toHaveBeenCalledWith(
      "staging:election_result_emitted:election_night:e-ca-3",
      "2026-06-03T00:00:00.000Z",
      { EX: 54_600 }
    );
  });

  it("skips election/pass rows that already have emitted markers", async () => {
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

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        exists: redisExistsMock,
        set: redisSetMock,
        quit: redisQuitMock,
        isOpen: true,
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

    redisExistsMock.mockResolvedValueOnce(1);
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "e-ca-1",
            state: "CA",
            election_date: "2026-06-02",
            election_night_results_checked_at: null,
            certified_results_checked_at: "2026-06-01T00:00:00.000Z",
            election_night_results_attempt_count: 0,
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ scheduled_at: new Date("2026-06-03T03:10:00.000Z") }] });

    const { runElectionResultScheduleProducer } = await import(
      "../../src/pipeline/producers/electionResultScheduleProducer.js"
    );

    const result = await runElectionResultScheduleProducer({ now: new Date("2026-06-03T00:00:00.000Z") });

    expect(result.dueElectionCount).toBe(0);
    expect(result.markerSkippedElectionCount).toBe(1);
    expect(queueAddMock).not.toHaveBeenCalled();
    expect(redisSetMock).not.toHaveBeenCalled();
  });

  it("does not schedule election-night again after the max attempt count", async () => {
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

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        exists: redisExistsMock,
        set: redisSetMock,
        quit: redisQuitMock,
        isOpen: true,
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

    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          election_id: "e-ca-1",
          state: "CA",
          election_date: "2026-06-02",
          election_night_results_checked_at: null,
          certified_results_checked_at: "2026-06-01T00:00:00.000Z",
          election_night_results_attempt_count: 3,
        },
      ],
    });

    const { runElectionResultScheduleProducer } = await import(
      "../../src/pipeline/producers/electionResultScheduleProducer.js"
    );

    const result = await runElectionResultScheduleProducer({ now: new Date("2026-06-03T00:00:00.000Z") });

    expect(result.dueElectionCount).toBe(0);
    expect(queueAddMock).not.toHaveBeenCalled();
    expect(redisSetMock).not.toHaveBeenCalled();
  });

  it("schedules certified retry when the previous certified attempt is at least seven days old", async () => {
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

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        exists: redisExistsMock,
        set: redisSetMock,
        quit: redisQuitMock,
        isOpen: true,
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

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "e-ca-1",
            state: "CA",
            election_date: "2026-06-02",
            election_night_results_checked_at: "2026-06-03T03:10:00.000Z",
            certified_results_checked_at: null,
            election_night_results_attempt_count: 1,
            certified_results_attempt_count: 1,
            certified_results_last_attempted_at: "2026-07-10T17:00:00.000Z",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ scheduled_at: new Date("2026-07-10T17:00:00.000Z") }] });

    const { runElectionResultScheduleProducer } = await import(
      "../../src/pipeline/producers/electionResultScheduleProducer.js"
    );

    const result = await runElectionResultScheduleProducer({ now: new Date("2026-07-17T17:01:00.000Z") });

    expect(result.dueElectionCount).toBe(1);
    expect(queueAddMock).toHaveBeenCalledWith(
      "election_result_search",
      expect.objectContaining({
        state: "CA",
        election_date: "2026-06-02",
        pass_type: "certified",
        election_ids: ["e-ca-1"],
      }),
      expect.objectContaining({
        jobId: "election-results:CA:2026-06-02:certified",
        delay: 0,
      })
    );
  });

  it("does not schedule certified retry before seven days or after the certified attempt cap", async () => {
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

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        exists: redisExistsMock,
        set: redisSetMock,
        quit: redisQuitMock,
        isOpen: true,
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

    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          election_id: "e-ca-1",
          state: "CA",
          election_date: "2026-06-02",
          election_night_results_checked_at: "2026-06-03T03:10:00.000Z",
          certified_results_checked_at: null,
          election_night_results_attempt_count: 1,
          certified_results_attempt_count: 1,
          certified_results_last_attempted_at: "2026-07-10T17:00:00.000Z",
        },
        {
          election_id: "e-ca-2",
          state: "CA",
          election_date: "2026-06-02",
          election_night_results_checked_at: "2026-06-03T03:10:00.000Z",
          certified_results_checked_at: null,
          election_night_results_attempt_count: 1,
          certified_results_attempt_count: 3,
          certified_results_last_attempted_at: "2026-07-03T17:00:00.000Z",
        },
      ],
    });

    const { runElectionResultScheduleProducer } = await import(
      "../../src/pipeline/producers/electionResultScheduleProducer.js"
    );

    const result = await runElectionResultScheduleProducer({ now: new Date("2026-07-16T17:01:00.000Z") });

    expect(result.dueElectionCount).toBe(0);
    expect(queueAddMock).not.toHaveBeenCalled();
    expect(redisSetMock).not.toHaveBeenCalled();
  });
});
