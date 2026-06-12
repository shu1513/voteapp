import { beforeEach, describe, expect, it, vi } from "vitest";

const queueMock = vi.hoisted(() =>
  vi.fn(() => ({
    upsertJobScheduler: vi.fn(async () => undefined),
    add: vi.fn(async () => ({ id: "manual-job-1" })),
    close: vi.fn(async () => undefined),
  }))
);

describe("presidentialPrimaryDateResearchScheduler", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    delete process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_SCHEDULER_QUEUE;
    delete process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_CRON;
    delete process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_TZ;

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

  it("passes dryRun/force through and preserves triggeredBy", async () => {
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(
        async (input: { dryRun?: boolean; force?: boolean }) => ({
          enabled: true,
          forced: Boolean(input.force),
          dryRun: Boolean(input.dryRun),
          now: "2027-03-07T00:00:00.000Z",
          maxRowsPerRun: 200,
          maxStatesPerJob: 20,
          maxJobsPerRun: 20,
          cyclesScanned: 2,
          eligibleCycleCount: 2,
          bootstrapRequestedRowCount: 102,
          bootstrapInsertedRowCount: 102,
          dueRowCount: 40,
          dueGroupCount: 2,
          selectedGroupCount: 2,
          maxRowsHit: false,
          maxGroupsHit: false,
          enqueuedJobCount: 2,
          updatedJobCount: 0,
          skippedActiveJobCount: 0,
        })
      ),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { runPresidentialPrimaryDateResearchRolloverJob } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await runPresidentialPrimaryDateResearchRolloverJob({
      dryRun: true,
      force: true,
      triggeredBy: "manual",
    });

    expect(result.dryRun).toBe(true);
    expect(result.force).toBe(true);
    expect(result.triggeredBy).toBe("manual");
    expect(result.enqueuedJobCount).toBe(2);
  });

  it("upserts the recurring daily scheduler with configured cron and timezone", async () => {
    process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_SCHEDULER_QUEUE = "ppd_maintenance_test";
    process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_CRON = "15 9 * * *";
    process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_TZ = "America/New_York";

    vi.doMock("bullmq", () => ({
      Queue: queueMock,
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { upsertRecurringPresidentialPrimaryDateResearchJobs } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    await upsertRecurringPresidentialPrimaryDateResearchJobs({ force: true, dryRun: true });

    expect(queueMock).toHaveBeenCalledWith(
      "ppd_maintenance_test",
      expect.objectContaining({
        defaultJobOptions: {
          removeOnComplete: 1000,
          removeOnFail: 1000,
        },
      })
    );
    const queueInstance = queueMock.mock.results[0]?.value;
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover",
      {
        pattern: "15 9 * * *",
        tz: "America/New_York",
      },
      expect.objectContaining({
        name: "presidential_primary_date_research_rollover",
        data: {
          dryRun: true,
          force: true,
          triggeredBy: "daily",
        },
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });

  it("enqueues manual scheduler jobs", async () => {
    vi.doMock("bullmq", () => ({
      Queue: queueMock,
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { enqueueManualPresidentialPrimaryDateResearchJob } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    await expect(
      enqueueManualPresidentialPrimaryDateResearchJob({ force: true, dryRun: true })
    ).resolves.toBe("manual-job-1");

    const queueInstance = queueMock.mock.results[0]?.value;
    expect(queueInstance.add).toHaveBeenCalledWith(
      "presidential_primary_date_research_rollover",
      expect.objectContaining({
        dryRun: true,
        force: true,
        triggeredBy: "manual",
      }),
      {
        removeOnComplete: 1000,
        removeOnFail: 1000,
      }
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
