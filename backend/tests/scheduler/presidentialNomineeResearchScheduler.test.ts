import { describe, expect, it, vi } from "vitest";

describe("runPresidentialNomineeResearchRolloverJob", () => {
  it("passes dryRun/force through and preserves triggeredBy", async () => {
    vi.resetModules();
    vi.doMock("../../src/pipeline/producers/presidentialNomineeResearchProducer.js", () => ({
      runPresidentialNomineeResearchProducer: vi.fn(
        async (input: { dryRun?: boolean; force?: boolean }) => ({
          enabled: true,
          forced: Boolean(input?.force),
          dryRun: Boolean(input?.dryRun),
          now: "2028-02-07T00:00:00.000Z",
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

    const { runPresidentialNomineeResearchRolloverJob } = await import(
      "../../src/scheduler/presidentialNomineeResearchScheduler.js"
    );

    const result = await runPresidentialNomineeResearchRolloverJob({
      dryRun: true,
      force: true,
      triggeredBy: "manual",
    });

    expect(result.dryRun).toBe(true);
    expect(result.forced).toBe(true);
    expect(result.triggeredBy).toBe("manual");
    expect(result.enqueuedJobCount).toBe(1);
  });
});
