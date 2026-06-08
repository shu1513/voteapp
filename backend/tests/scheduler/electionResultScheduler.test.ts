import { describe, expect, it, vi } from "vitest";

describe("runElectionResultScheduleRolloverJob", () => {
  it("passes dryRun/force through and preserves triggeredBy", async () => {
    vi.resetModules();
    vi.doMock("../../src/pipeline/producers/electionResultScheduleProducer.js", () => ({
      runElectionResultScheduleProducer: vi.fn(async (input: { dryRun?: boolean; force?: boolean }) => ({
        enabled: true,
        forced: Boolean(input?.force),
        dryRun: Boolean(input?.dryRun),
        now: "2026-06-03T00:00:00.000Z",
        lookaheadHours: 48,
        maxGroupsPerRun: 200,
        electionsScanned: 12,
        dueElectionCount: 10,
        dueGroupCount: 2,
        selectedGroupCount: 2,
        maxGroupsHit: false,
        enqueuedJobCount: 1,
        updatedJobCount: 1,
        skippedActiveJobCount: 0,
        skippedUnknownStateCount: 0,
      })),
    }));

    const { runElectionResultScheduleRolloverJob } = await import(
      "../../src/scheduler/electionResultScheduler.js"
    );

    const result = await runElectionResultScheduleRolloverJob({
      dryRun: true,
      force: true,
      triggeredBy: "manual",
    });

    expect(result.dryRun).toBe(true);
    expect(result.force).toBe(true);
    expect(result.triggeredBy).toBe("manual");
    expect(result.enqueuedJobCount).toBe(1);
    expect(result.updatedJobCount).toBe(1);
  });
});
