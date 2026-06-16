import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const queueMock = vi.hoisted(() =>
  vi.fn(() => ({
    upsertJobScheduler: vi.fn(async () => undefined),
    removeJobScheduler: vi.fn(async () => true),
    getJob: vi.fn(async () => undefined),
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
    delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;

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

  afterEach(() => {
    vi.useRealTimers();
  });

  it("passes dryRun/force through, preserves triggeredBy, and syncs around active work", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2027-03-08T00:00:00.000Z"));

    const poolEnd = vi.fn(async () => undefined);
    const queueClose = vi.fn(async () => undefined);
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => undefined),
      add: vi.fn(async () => ({ id: "activation" })),
      close: queueClose,
    };
    const producerMock = vi.fn(
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
    );

    try {
      vi.doMock("pg", () => ({
        Pool: vi.fn(() => ({
          query: vi.fn(async () => ({
            rows: [
              {
                cycle_id: "cycle-2028-democratic",
                election_year: 2028,
                official_found_count: "50",
              },
            ],
          })),
          end: poolEnd,
        })),
      }));
      vi.doMock("bullmq", () => ({
        Queue: vi.fn(() => queueInstance),
        Worker: vi.fn(),
      }));
      vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
        runPresidentialPrimaryDateResearchProducer: producerMock,
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
      expect(result.schedulerState?.mode).toBe("active");
      expect(result.schedulerSync).toEqual({
        dailyScheduler: "upserted",
        activationJob: "none",
        activationScheduledFor: null,
        completionJob: "scheduled",
        completionScheduledFor: "2028-12-07T00:00:00.000Z",
      });
      expect(result.enqueuedJobCount).toBe(2);
      expect(producerMock).toHaveBeenCalledWith({
        dryRun: true,
        force: true,
        now: new Date("2027-03-08T00:00:00.000Z"),
      });
      expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(2);
      expect(queueClose).toHaveBeenCalledTimes(1);
      expect(poolEnd).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("returns a disabled rollover result without touching DB or Redis when the master flag is off", async () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    const Pool = vi.fn();
    const Queue = vi.fn();
    const producerMock = vi.fn();
    vi.doMock("pg", () => ({ Pool }));
    vi.doMock("bullmq", () => ({
      Queue,
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: producerMock,
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

    expect(result).toMatchObject({
      enabled: false,
      force: true,
      triggeredBy: "manual",
      cyclesScanned: 0,
      dueRowCount: 0,
      enqueuedJobCount: 0,
      schedulerState: {
        mode: "complete",
        cycleCount: 0,
      },
      schedulerSync: {
        dailyScheduler: "disabled",
        activationJob: "none",
        completionJob: "none",
      },
    });
    expect(Pool).not.toHaveBeenCalled();
    expect(Queue).not.toHaveBeenCalled();
    expect(producerMock).not.toHaveBeenCalled();
  });

  it("uses one rollover timestamp for initial and final adaptive sync", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2028-12-06T23:59:59.999Z"));

    const poolEnd = vi.fn(async () => undefined);
    const queueClose = vi.fn(async () => undefined);
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => undefined),
      add: vi.fn(async () => ({ id: "completion" })),
      close: queueClose,
    };
    const producerMock = vi.fn(async () => {
      vi.setSystemTime(new Date("2028-12-07T00:00:00.000Z"));
      return {
        enabled: true,
        forced: false,
        dryRun: false,
        now: "2028-12-06T23:59:59.999Z",
        maxRowsPerRun: 200,
        maxStatesPerJob: 10,
        maxJobsPerRun: 20,
        cyclesScanned: 1,
        eligibleCycleCount: 1,
        bootstrapRequestedRowCount: 0,
        bootstrapInsertedRowCount: 0,
        dueRowCount: 1,
        dueGroupCount: 1,
        selectedGroupCount: 1,
        maxRowsHit: false,
        maxGroupsHit: false,
        enqueuedJobCount: 1,
        updatedJobCount: 0,
        skippedActiveJobCount: 0,
      };
    });

    try {
      vi.doMock("pg", () => ({
        Pool: vi.fn(() => ({
          query: vi.fn(async () => ({
            rows: [
              {
                cycle_id: "cycle-2028-democratic",
                election_year: 2028,
                official_found_count: "50",
              },
            ],
          })),
          end: poolEnd,
        })),
      }));
      vi.doMock("bullmq", () => ({
        Queue: vi.fn(() => queueInstance),
        Worker: vi.fn(),
      }));
      vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
        runPresidentialPrimaryDateResearchProducer: producerMock,
        PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
      }));

      const { runPresidentialPrimaryDateResearchRolloverJob } = await import(
        "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
      );

      const result = await runPresidentialPrimaryDateResearchRolloverJob({
        triggeredBy: "daily",
      });

      expect(result.schedulerState?.mode).toBe("active");
      expect(result.schedulerSync).toEqual({
        dailyScheduler: "upserted",
        activationJob: "none",
        activationScheduledFor: null,
        completionJob: "scheduled",
        completionScheduledFor: "2028-12-07T00:00:00.000Z",
      });
      expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(2);
      expect(queueInstance.removeJobScheduler).not.toHaveBeenCalledWith(
        "presidential_primary_date_research_daily_rollover"
      );
      expect(queueInstance.add).toHaveBeenCalledWith(
        "presidential_primary_date_research_rollover",
        expect.objectContaining({
          triggeredBy: "completion",
          requestedAt: "2028-12-06T23:59:59.999Z",
        }),
        expect.objectContaining({
          delay: 1,
          jobId: "presidential_primary_date_research_completion",
        })
      );
      expect(queueClose).toHaveBeenCalledTimes(1);
      expect(poolEnd).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("activation jobs sync the scheduler and run the producer when work is active", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2027-03-08T00:00:00.000Z"));

    const poolEnd = vi.fn(async () => undefined);
    const queueClose = vi.fn(async () => undefined);
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => ({
        getState: vi.fn(async () => "active"),
        remove: vi.fn(async () => undefined),
      })),
      add: vi.fn(async () => ({ id: "activation" })),
      close: queueClose,
    };
    const producerMock = vi.fn(async (input: { dryRun?: boolean; force?: boolean }) => ({
      enabled: true,
      forced: Boolean(input.force),
      dryRun: Boolean(input.dryRun),
      now: "2027-03-08T00:00:00.000Z",
      maxRowsPerRun: 200,
      maxStatesPerJob: 10,
      maxJobsPerRun: 20,
      cyclesScanned: 1,
      eligibleCycleCount: 1,
      bootstrapRequestedRowCount: 51,
      bootstrapInsertedRowCount: 0,
      dueRowCount: 1,
      dueGroupCount: 1,
      selectedGroupCount: 1,
      maxRowsHit: false,
      maxGroupsHit: false,
      enqueuedJobCount: 1,
      updatedJobCount: 0,
      skippedActiveJobCount: 0,
    }));

    try {
      vi.doMock("pg", () => ({
        Pool: vi.fn(() => ({
          query: vi.fn(async () => ({
            rows: [
              {
                cycle_id: "cycle-2028-democratic",
                election_year: 2028,
                official_found_count: "50",
              },
            ],
          })),
          end: poolEnd,
        })),
      }));
      vi.doMock("bullmq", () => ({
        Queue: vi.fn(() => queueInstance),
        Worker: vi.fn(),
      }));
      vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
        runPresidentialPrimaryDateResearchProducer: producerMock,
        PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
      }));

      const { runPresidentialPrimaryDateResearchRolloverJob } = await import(
        "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
      );

      const result = await runPresidentialPrimaryDateResearchRolloverJob({
        dryRun: true,
        force: true,
        triggeredBy: "activation",
      });

      expect(result.triggeredBy).toBe("activation");
      expect(result.schedulerState?.mode).toBe("active");
      expect(result.schedulerSync).toEqual({
        dailyScheduler: "upserted",
        activationJob: "none",
        activationScheduledFor: null,
        completionJob: "scheduled",
        completionScheduledFor: "2028-12-07T00:00:00.000Z",
      });
      expect(result.enqueuedJobCount).toBe(1);
      expect(queueInstance.upsertJobScheduler).toHaveBeenCalledWith(
        "presidential_primary_date_research_daily_rollover",
        expect.any(Object),
        expect.objectContaining({
          data: {
            dryRun: true,
            force: true,
            triggeredBy: "daily",
          },
        })
      );
      expect(producerMock).toHaveBeenCalledWith({
        dryRun: true,
        force: true,
        now: new Date("2027-03-08T00:00:00.000Z"),
      });
      expect(queueClose).toHaveBeenCalledTimes(1);
      expect(poolEnd).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("activation jobs reschedule without running the producer when the window is still closed", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-12T00:00:00.000Z"));

    const poolEnd = vi.fn(async () => undefined);
    const queueClose = vi.fn(async () => undefined);
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => ({
        getState: vi.fn(async () => "active"),
        remove: vi.fn(async () => undefined),
      })),
      add: vi.fn(async () => ({ id: "activation-rescheduled" })),
      close: queueClose,
    };
    const producerMock = vi.fn();

    try {
      vi.doMock("pg", () => ({
        Pool: vi.fn(() => ({
          query: vi.fn(async () => ({
            rows: [
              {
                cycle_id: "cycle-2028-democratic",
                election_year: 2028,
                official_found_count: "0",
              },
            ],
          })),
          end: poolEnd,
        })),
      }));
      vi.doMock("bullmq", () => ({
        Queue: vi.fn(() => queueInstance),
        Worker: vi.fn(),
      }));
      vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
        runPresidentialPrimaryDateResearchProducer: producerMock,
        PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
      }));

      const { runPresidentialPrimaryDateResearchRolloverJob } = await import(
        "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
      );

      const result = await runPresidentialPrimaryDateResearchRolloverJob({
        triggeredBy: "activation",
      });

      expect(result.triggeredBy).toBe("activation");
      expect(result.schedulerState?.mode).toBe("sleep_until");
      expect(result.enqueuedJobCount).toBe(0);
      expect(producerMock).not.toHaveBeenCalled();
      expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
        "presidential_primary_date_research_daily_rollover"
      );
      expect(queueInstance.add).toHaveBeenCalledWith(
        "presidential_primary_date_research_rollover",
        expect.objectContaining({
          triggeredBy: "activation",
          requestedAt: "2026-06-12T00:00:00.000Z",
        }),
        expect.objectContaining({
          jobId:
            "presidential_primary_date_research_activation:2027-03-07T00:00:00.000Z",
          delay: expect.any(Number),
        })
      );
      expect(queueClose).toHaveBeenCalledTimes(1);
      expect(poolEnd).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("daily jobs sync and skip producer work when every active primary date is official", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2027-03-08T00:00:00.000Z"));

    const poolEnd = vi.fn(async () => undefined);
    const queueClose = vi.fn(async () => undefined);
    const removeActivation = vi.fn(async () => undefined);
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => ({ remove: removeActivation })),
      add: vi.fn(async () => ({ id: "activation" })),
      close: queueClose,
    };
    const producerMock = vi.fn();

    try {
      vi.doMock("pg", () => ({
        Pool: vi.fn(() => ({
          query: vi.fn(async () => ({
            rows: [
              {
                cycle_id: "cycle-2028-democratic",
                election_year: 2028,
                official_found_count: "51",
              },
            ],
          })),
          end: poolEnd,
        })),
      }));
      vi.doMock("bullmq", () => ({
        Queue: vi.fn(() => queueInstance),
        Worker: vi.fn(),
      }));
      vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
        runPresidentialPrimaryDateResearchProducer: producerMock,
        PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
      }));

      const { runPresidentialPrimaryDateResearchRolloverJob } = await import(
        "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
      );

      const result = await runPresidentialPrimaryDateResearchRolloverJob({
        triggeredBy: "daily",
      });

      expect(result.triggeredBy).toBe("daily");
      expect(result.schedulerState?.mode).toBe("complete");
      expect(result.schedulerSync).toEqual({
        dailyScheduler: "removed",
        activationJob: "removed",
        activationScheduledFor: null,
        completionJob: "scheduled",
        completionScheduledFor: "2028-12-07T00:00:00.000Z",
      });
      expect(result.enqueuedJobCount).toBe(0);
      expect(producerMock).not.toHaveBeenCalled();
      expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
        "presidential_primary_date_research_daily_rollover"
      );
      expect(removeActivation).toHaveBeenCalledTimes(2);
      expect(queueClose).toHaveBeenCalledTimes(1);
      expect(poolEnd).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("daily jobs schedule activation and skip producer work before the research window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-12T00:00:00.000Z"));

    const poolEnd = vi.fn(async () => undefined);
    const queueClose = vi.fn(async () => undefined);
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => undefined),
      add: vi.fn(async () => ({ id: "activation" })),
      close: queueClose,
    };
    const producerMock = vi.fn();

    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        query: vi.fn(async () => ({
          rows: [
            {
              cycle_id: "cycle-2028-republican",
              election_year: 2028,
              official_found_count: "0",
            },
          ],
        })),
        end: poolEnd,
      })),
    }));
    vi.doMock("bullmq", () => ({
      Queue: vi.fn(() => queueInstance),
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: producerMock,
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { runPresidentialPrimaryDateResearchRolloverJob } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await runPresidentialPrimaryDateResearchRolloverJob({
      triggeredBy: "daily",
    });

    expect(result.triggeredBy).toBe("daily");
    expect(result.schedulerState?.mode).toBe("sleep_until");
    expect(result.schedulerSync).toEqual({
      dailyScheduler: "removed",
      activationJob: "scheduled",
      activationScheduledFor: "2027-03-07T00:00:00.000Z",
      completionJob: "scheduled",
      completionScheduledFor: "2028-12-07T00:00:00.000Z",
    });
    expect(result.enqueuedJobCount).toBe(0);
    expect(producerMock).not.toHaveBeenCalled();
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover"
    );
    expect(queueInstance.add).toHaveBeenCalledWith(
      "presidential_primary_date_research_rollover",
      expect.objectContaining({
        triggeredBy: "activation",
        requestedAt: "2026-06-12T00:00:00.000Z",
      }),
      expect.objectContaining({
        jobId: "presidential_primary_date_research_activation",
        delay: expect.any(Number),
      })
    );
    expect(queueClose).toHaveBeenCalledTimes(1);
    expect(poolEnd).toHaveBeenCalledTimes(1);
  });

  it("final sync disables daily scheduling when producer work completes the remaining dates", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2027-03-08T00:00:00.000Z"));

    const poolEnd = vi.fn(async () => undefined);
    const queueClose = vi.fn(async () => undefined);
    const removeActivation = vi.fn(async () => undefined);
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            cycle_id: "cycle-2028-democratic",
            election_year: 2028,
            official_found_count: "50",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            cycle_id: "cycle-2028-democratic",
            election_year: 2028,
            official_found_count: "51",
          },
        ],
      });
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => ({ remove: removeActivation })),
      add: vi.fn(async () => ({ id: "activation" })),
      close: queueClose,
    };
    const producerMock = vi.fn(async (input: { dryRun?: boolean; force?: boolean }) => ({
      enabled: true,
      forced: Boolean(input.force),
      dryRun: Boolean(input.dryRun),
      now: "2027-03-08T00:00:00.000Z",
      maxRowsPerRun: 200,
      maxStatesPerJob: 10,
      maxJobsPerRun: 20,
      cyclesScanned: 1,
      eligibleCycleCount: 1,
      bootstrapRequestedRowCount: 51,
      bootstrapInsertedRowCount: 0,
      dueRowCount: 1,
      dueGroupCount: 1,
      selectedGroupCount: 1,
      maxRowsHit: false,
      maxGroupsHit: false,
      enqueuedJobCount: 1,
      updatedJobCount: 0,
      skippedActiveJobCount: 0,
    }));

    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        query,
        end: poolEnd,
      })),
    }));
    vi.doMock("bullmq", () => ({
      Queue: vi.fn(() => queueInstance),
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: producerMock,
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { runPresidentialPrimaryDateResearchRolloverJob } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await runPresidentialPrimaryDateResearchRolloverJob({
      triggeredBy: "daily",
    });

    expect(result.schedulerState?.mode).toBe("active");
    expect(result.schedulerSync).toEqual({
      dailyScheduler: "removed",
      activationJob: "removed",
      activationScheduledFor: null,
      completionJob: "scheduled",
      completionScheduledFor: "2028-12-07T00:00:00.000Z",
    });
    expect(result.enqueuedJobCount).toBe(1);
    expect(producerMock).toHaveBeenCalledTimes(1);
    expect(queueInstance.upsertJobScheduler).toHaveBeenCalledTimes(1);
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover"
    );
    expect(removeActivation).toHaveBeenCalledTimes(4);
    expect(query).toHaveBeenCalledTimes(2);
    expect(queueClose).toHaveBeenCalledTimes(1);
    expect(poolEnd).toHaveBeenCalledTimes(1);
  });

  it("reports complete when no active primary cycles need dates", async () => {
    const query = vi.fn(async () => ({ rows: [] }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { loadPresidentialPrimaryDateResearchSchedulerState } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    await expect(
      loadPresidentialPrimaryDateResearchSchedulerState(
        { query },
        new Date("2027-03-08T00:00:00.000Z")
      )
    ).resolves.toEqual({
      mode: "complete",
      now: "2027-03-08T00:00:00.000Z",
      cycleCount: 0,
      incompleteCycleCount: 0,
      activeMissingCycleCount: 0,
      expiredCycleCount: 0,
      expiredIncompleteCycleCount: 0,
      missingStatePartyRowCount: 0,
      expiredMissingStatePartyRowCount: 0,
      nextActivationAt: null,
      nextCompletionAt: null,
    });
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("reports active when an incomplete primary cycle is inside its research window", async () => {
    const query = vi.fn(async () => ({
      rows: [
        {
          cycle_id: "cycle-2028-democratic",
          election_year: 2028,
          row_count: "51",
          official_found_count: "50",
        },
      ],
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { loadPresidentialPrimaryDateResearchSchedulerState } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    await expect(
      loadPresidentialPrimaryDateResearchSchedulerState(
        { query },
        new Date("2027-03-08T00:00:00.000Z")
      )
    ).resolves.toMatchObject({
      mode: "active",
      cycleCount: 1,
      incompleteCycleCount: 1,
      activeMissingCycleCount: 1,
      missingStatePartyRowCount: 1,
      nextActivationAt: null,
    });
  });

  it("reports sleep_until when the next incomplete primary cycle is before its research window", async () => {
    const query = vi.fn(async () => ({
      rows: [
        {
          cycle_id: "cycle-2028-republican",
          election_year: 2028,
          row_count: "0",
          official_found_count: "0",
        },
      ],
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { loadPresidentialPrimaryDateResearchSchedulerState } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    await expect(
      loadPresidentialPrimaryDateResearchSchedulerState(
        { query },
        new Date("2026-06-12T00:00:00.000Z")
      )
    ).resolves.toMatchObject({
      mode: "sleep_until",
      cycleCount: 1,
      incompleteCycleCount: 1,
      activeMissingCycleCount: 0,
      missingStatePartyRowCount: 51,
      nextActivationAt: "2027-03-07T00:00:00.000Z",
    });
  });

  it("ignores complete cycles while choosing the next activation date", async () => {
    const query = vi.fn(async () => ({
      rows: [
        {
          cycle_id: "cycle-2028-democratic",
          election_year: 2028,
          row_count: "51",
          official_found_count: "51",
        },
        {
          cycle_id: "cycle-2032-democratic",
          election_year: 2032,
          row_count: "10",
          official_found_count: "10",
        },
      ],
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { loadPresidentialPrimaryDateResearchSchedulerState } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    await expect(
      loadPresidentialPrimaryDateResearchSchedulerState(
        { query },
        new Date("2028-01-01T00:00:00.000Z")
      )
    ).resolves.toMatchObject({
      mode: "sleep_until",
      cycleCount: 2,
      incompleteCycleCount: 1,
      activeMissingCycleCount: 0,
      missingStatePartyRowCount: 41,
      nextActivationAt: "2031-03-02T00:00:00.000Z",
    });
  });

  it("does not keep the scheduler active for expired incomplete primary cycles", async () => {
    const query = vi.fn(async () => ({
      rows: [
        {
          cycle_id: "cycle-2028-democratic",
          election_year: 2028,
          official_found_count: "50",
        },
      ],
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { loadPresidentialPrimaryDateResearchSchedulerState } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    await expect(
      loadPresidentialPrimaryDateResearchSchedulerState(
        { query },
        new Date("2028-12-07T00:00:00.000Z")
      )
    ).resolves.toMatchObject({
      mode: "complete",
      cycleCount: 1,
      incompleteCycleCount: 0,
      activeMissingCycleCount: 0,
      expiredCycleCount: 1,
      expiredIncompleteCycleCount: 1,
      missingStatePartyRowCount: 0,
      expiredMissingStatePartyRowCount: 1,
      nextActivationAt: null,
    });
  });

  it("syncs active research state by enabling daily scheduling and removing stale activation", async () => {
    const removeActivation = vi.fn(async () => undefined);
    const queue = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => ({ remove: removeActivation })),
      add: vi.fn(async () => ({ id: "activation" })),
    };
    const query = vi.fn(async () => ({
      rows: [
        {
          cycle_id: "cycle-2028-democratic",
          election_year: 2028,
          official_found_count: "50",
        },
      ],
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { syncPresidentialPrimaryDateResearchScheduler } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await syncPresidentialPrimaryDateResearchScheduler(
      { query },
      queue,
      {
        dryRun: true,
        force: true,
        now: new Date("2027-03-08T00:00:00.000Z"),
      }
    );

    expect(result).toMatchObject({
      dailyScheduler: "upserted",
      activationJob: "removed",
      activationScheduledFor: null,
      state: {
        mode: "active",
      },
    });
    expect(queue.upsertJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover",
      expect.any(Object),
      expect.objectContaining({
        data: {
          dryRun: true,
          force: true,
          triggeredBy: "daily",
        },
      })
    );
    expect(removeActivation).toHaveBeenCalledTimes(2);
    expect(queue.removeJobScheduler).not.toHaveBeenCalled();
    expect(queue.add).toHaveBeenCalledWith(
      "presidential_primary_date_research_rollover",
      expect.objectContaining({
        triggeredBy: "completion",
      }),
      expect.objectContaining({
        jobId: "presidential_primary_date_research_completion",
        delay: expect.any(Number),
      })
    );
  });

  it("syncs sleep_until research state by removing daily scheduling and scheduling activation", async () => {
    const removeActivation = vi.fn(async () => undefined);
    const queue = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => ({ remove: removeActivation })),
      add: vi.fn(async () => ({ id: "activation" })),
    };
    const query = vi.fn(async () => ({
      rows: [
        {
          cycle_id: "cycle-2028-republican",
          election_year: 2028,
          official_found_count: "0",
        },
      ],
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { syncPresidentialPrimaryDateResearchScheduler } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await syncPresidentialPrimaryDateResearchScheduler(
      { query },
      queue,
      {
        now: new Date("2026-06-12T00:00:00.000Z"),
      }
    );

    expect(result).toMatchObject({
      dailyScheduler: "removed",
      activationJob: "scheduled",
      activationScheduledFor: "2027-03-07T00:00:00.000Z",
      state: {
        mode: "sleep_until",
      },
    });
    expect(queue.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover"
    );
    expect(removeActivation).toHaveBeenCalledTimes(2);
    expect(queue.add).toHaveBeenCalledWith(
      "presidential_primary_date_research_rollover",
      {
        dryRun: false,
        force: false,
        triggeredBy: "activation",
        requestedAt: "2026-06-12T00:00:00.000Z",
      },
      expect.objectContaining({
        jobId: "presidential_primary_date_research_activation",
        delay: expect.any(Number),
      })
    );
    expect(queue.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it("syncs complete research state by removing daily scheduling and stale activation", async () => {
    const removeActivation = vi.fn(async () => undefined);
    const queue = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => ({ remove: removeActivation })),
      add: vi.fn(async () => ({ id: "activation" })),
    };
    const query = vi.fn(async () => ({
      rows: [
        {
          cycle_id: "cycle-2028-democratic",
          election_year: 2028,
          official_found_count: "51",
        },
      ],
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { syncPresidentialPrimaryDateResearchScheduler } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await syncPresidentialPrimaryDateResearchScheduler(
      { query },
      queue,
      {
        now: new Date("2027-03-08T00:00:00.000Z"),
      }
    );

    expect(result).toMatchObject({
      dailyScheduler: "removed",
      activationJob: "removed",
      activationScheduledFor: null,
      state: {
        mode: "complete",
      },
    });
    expect(queue.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover"
    );
    expect(removeActivation).toHaveBeenCalledTimes(2);
    expect(queue.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queue.add).toHaveBeenCalledWith(
      "presidential_primary_date_research_rollover",
      expect.objectContaining({
        triggeredBy: "completion",
      }),
      expect.objectContaining({
        jobId: "presidential_primary_date_research_completion",
        delay: expect.any(Number),
      })
    );
  });

  it("adaptive scheduler setup enables daily scheduling when active work exists", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2027-03-08T00:00:00.000Z"));
    process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_SCHEDULER_QUEUE = "ppd_maintenance_test";
    process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_CRON = "15 9 * * *";
    process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_TZ = "America/New_York";

    const poolEnd = vi.fn(async () => undefined);
    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        query: vi.fn(async () => ({
          rows: [
            {
              cycle_id: "cycle-2028-democratic",
              election_year: 2028,
              official_found_count: "50",
            },
          ],
        })),
        end: poolEnd,
      })),
    }));
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

    const result = await upsertRecurringPresidentialPrimaryDateResearchJobs({ force: true, dryRun: true });

    expect(result.state.mode).toBe("active");
    expect(result.dailyScheduler).toBe("upserted");
    expect(result.activationJob).toBe("none");
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
    expect(poolEnd).toHaveBeenCalledTimes(1);
    vi.useRealTimers();
  });

  it("adaptive scheduler setup schedules activation instead of daily work before the research window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-12T00:00:00.000Z"));
    const poolEnd = vi.fn(async () => undefined);
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => undefined),
      add: vi.fn(async () => ({ id: "activation" })),
      close: vi.fn(async () => undefined),
    };

    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        query: vi.fn(async () => ({
          rows: [
            {
              cycle_id: "cycle-2028-republican",
              election_year: 2028,
              official_found_count: "0",
            },
          ],
        })),
        end: poolEnd,
      })),
    }));
    vi.doMock("bullmq", () => ({
      Queue: vi.fn(() => queueInstance),
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { upsertRecurringPresidentialPrimaryDateResearchJobs } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await upsertRecurringPresidentialPrimaryDateResearchJobs();

    expect(result.state.mode).toBe("sleep_until");
    expect(result.dailyScheduler).toBe("removed");
    expect(result.activationJob).toBe("scheduled");
    expect(result.activationScheduledFor).toBe("2027-03-07T00:00:00.000Z");
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover"
    );
    expect(queueInstance.add).toHaveBeenCalledWith(
      "presidential_primary_date_research_rollover",
      expect.objectContaining({
        triggeredBy: "activation",
      }),
      expect.objectContaining({
        jobId: "presidential_primary_date_research_activation",
        delay: expect.any(Number),
      })
    );
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
    expect(poolEnd).toHaveBeenCalledTimes(1);
    vi.useRealTimers();
  });

  it("adaptive scheduler setup removes scheduled work when all known primary dates are complete", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2027-03-08T00:00:00.000Z"));

    const poolEnd = vi.fn(async () => undefined);
    const removeActivation = vi.fn(async () => undefined);
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => ({ remove: removeActivation })),
      add: vi.fn(async () => ({ id: "activation" })),
      close: vi.fn(async () => undefined),
    };

    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        query: vi.fn(async () => ({
          rows: [
            {
              cycle_id: "cycle-2028-republican",
              election_year: 2028,
              official_found_count: "51",
            },
          ],
        })),
        end: poolEnd,
      })),
    }));
    vi.doMock("bullmq", () => ({
      Queue: vi.fn(() => queueInstance),
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { upsertRecurringPresidentialPrimaryDateResearchJobs } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await upsertRecurringPresidentialPrimaryDateResearchJobs();

    expect(result.state.mode).toBe("complete");
    expect(result.dailyScheduler).toBe("removed");
    expect(result.activationJob).toBe("removed");
    expect(result.activationScheduledFor).toBeNull();
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover"
    );
    expect(removeActivation).toHaveBeenCalledTimes(2);
    expect(queueInstance.upsertJobScheduler).not.toHaveBeenCalled();
    expect(queueInstance.add).toHaveBeenCalledWith(
      "presidential_primary_date_research_rollover",
      expect.objectContaining({
        triggeredBy: "completion",
      }),
      expect.objectContaining({
        jobId: "presidential_primary_date_research_completion",
        delay: expect.any(Number),
      })
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
    expect(poolEnd).toHaveBeenCalledTimes(1);
  });

  it("completion jobs mark expired primary cycles completed before syncing scheduler state", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2028-12-07T00:00:00.000Z"));

    const poolEnd = vi.fn(async () => undefined);
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [{ id: "cycle-2028-democratic", election_year: 2028 }],
      })
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ id: "cycle-2028-democratic" }],
      })
      .mockResolvedValueOnce({ rows: [] });
    const queueInstance = {
      upsertJobScheduler: vi.fn(async () => undefined),
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => undefined),
      add: vi.fn(async () => ({ id: "completion" })),
      close: vi.fn(async () => undefined),
    };
    const producerMock = vi.fn();

    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        query,
        end: poolEnd,
      })),
    }));
    vi.doMock("bullmq", () => ({
      Queue: vi.fn(() => queueInstance),
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: producerMock,
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { runPresidentialPrimaryDateResearchRolloverJob } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    const result = await runPresidentialPrimaryDateResearchRolloverJob({
      triggeredBy: "completion",
    });

    expect(result.triggeredBy).toBe("completion");
    expect(result.cycleCompletion).toEqual({
      dryRun: false,
      now: "2028-12-07T00:00:00.000Z",
      scannedCycleCount: 1,
      expiredCycleCount: 1,
      completedCycleCount: 1,
      completedCycleIds: ["cycle-2028-democratic"],
    });
    expect(result.schedulerState?.mode).toBe("complete");
    expect(result.schedulerSync).toEqual({
      dailyScheduler: "removed",
      activationJob: "none",
      activationScheduledFor: null,
      completionJob: "none",
      completionScheduledFor: null,
    });
    expect(producerMock).not.toHaveBeenCalled();
    expect(String(query.mock.calls[1]?.[0])).toContain("SET status = 'completed'");
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover"
    );
    expect(queueInstance.add).not.toHaveBeenCalled();
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
    expect(poolEnd).toHaveBeenCalledTimes(1);

    vi.useRealTimers();
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

  it("does not enqueue manual jobs and removes scheduled work when the master flag is off", async () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    const queueInstance = {
      removeJobScheduler: vi.fn(async () => true),
      getJob: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const Queue = vi.fn(() => queueInstance);
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));
    vi.doMock("bullmq", () => ({
      Queue,
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const {
      enqueueManualPresidentialPrimaryDateResearchJob,
      upsertRecurringPresidentialPrimaryDateResearchJobs,
    } = await import("../../src/scheduler/presidentialPrimaryDateResearchScheduler.js");

    await expect(enqueueManualPresidentialPrimaryDateResearchJob({ force: true })).resolves.toBe("disabled");
    await expect(upsertRecurringPresidentialPrimaryDateResearchJobs({ force: true })).resolves.toMatchObject({
      state: {
        mode: "complete",
        cycleCount: 0,
      },
      dailyScheduler: "disabled",
    });
    expect(Queue).toHaveBeenCalledTimes(1);
    expect(queueInstance.removeJobScheduler).toHaveBeenCalledWith(
      "presidential_primary_date_research_daily_rollover"
    );
    expect(queueInstance.getJob).toHaveBeenCalledWith(
      "presidential_primary_date_research_activation"
    );
    expect(queueInstance.getJob).toHaveBeenCalledWith(
      "presidential_primary_date_research_completion"
    );
    expect(queueInstance.close).toHaveBeenCalledTimes(1);
    expect(Pool).not.toHaveBeenCalled();
  });

  it("rejects unsupported Redis URL protocols before creating the scheduler queue", async () => {
    vi.doMock("bullmq", () => ({
      Queue: queueMock,
      Worker: vi.fn(),
    }));
    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "postgresql://localhost:5432/not-redis",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));
    vi.doMock("../../src/pipeline/producers/presidentialPrimaryDateResearchProducer.js", () => ({
      runPresidentialPrimaryDateResearchProducer: vi.fn(),
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME: "presidential_primary_date_research",
    }));

    const { createPresidentialPrimaryDateResearchSchedulerQueue } = await import(
      "../../src/scheduler/presidentialPrimaryDateResearchScheduler.js"
    );

    expect(() => createPresidentialPrimaryDateResearchSchedulerQueue()).toThrow(
      "Unsupported REDIS_URL protocol: postgresql:"
    );
    expect(queueMock).not.toHaveBeenCalled();
  });
});
