import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("presidentialRosterResearchScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
  });

  afterEach(() => {
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("passes dryRun/force through and preserves triggeredBy", async () => {
    vi.doMock("../../src/pipeline/producers/presidentialRosterResearchProducer.js", () => ({
      runPresidentialRosterResearchProducer: vi.fn(
        async (input: { dryRun?: boolean; force?: boolean }) => ({
          enabled: true,
          forced: Boolean(input?.force),
          dryRun: Boolean(input?.dryRun),
          now: "2027-03-07T00:00:00.000Z",
          maxCyclesPerRun: 10,
          cyclesScanned: 2,
          dueCycleCount: 1,
          selectedCycleCount: 1,
          maxCyclesHit: false,
          enqueuedJobCount: 1,
          updatedJobCount: 0,
          skippedActiveJobCount: 0,
        })
      ),
    }));

    const { runPresidentialRosterResearchRolloverJob } = await import(
      "../../src/scheduler/presidentialRosterResearchScheduler.js"
    );

    const result = await runPresidentialRosterResearchRolloverJob({
      dryRun: true,
      force: true,
      triggeredBy: "manual",
    });

    expect(result.dryRun).toBe(true);
    expect(result.forced).toBe(true);
    expect(result.triggeredBy).toBe("manual");
    expect(result.enqueuedJobCount).toBe(1);
  });

  it("does not enqueue manual jobs and removes the recurring scheduler when the master flag is off", async () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    const queueInstance = {
      removeJobScheduler: vi.fn(async () => true),
      close: vi.fn(async () => undefined),
    };
    const Queue = vi.fn(() => queueInstance);
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
    vi.doMock("bullmq", () => ({
      Queue,
      Worker: vi.fn(),
    }));

    const {
      enqueueManualPresidentialRosterResearchJob,
      upsertRecurringPresidentialRosterResearchJobs,
    } = await import("../../src/scheduler/presidentialRosterResearchScheduler.js");

    await expect(enqueueManualPresidentialRosterResearchJob({ force: true })).resolves.toBe("disabled");
    await expect(upsertRecurringPresidentialRosterResearchJobs({ force: true })).resolves.toBeUndefined();
    expect(Queue).toHaveBeenCalledTimes(1);
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_roster_research_daily_rollover"
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
  });
});
