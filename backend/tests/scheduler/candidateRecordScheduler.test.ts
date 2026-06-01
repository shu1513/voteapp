import { describe, expect, it, vi } from "vitest";

describe("runCandidateRecordRolloverJob", () => {
  it("passes force through and preserves triggeredBy", async () => {
    vi.resetModules();
    vi.doMock("../../src/pipeline/producers/candidateRecordRolloverProducer.js", () => ({
      runCandidateRecordRolloverProducer: vi.fn(async (input: { force?: boolean }) => ({
        enabled: true,
        forced: Boolean(input?.force),
        asOfDate: "2026-05-31",
        cooldownDays: 30,
        maxEnqueuePerRun: 2000,
        dueRows: 15,
        selectedRows: 15,
        maxEnqueueHit: false,
        emittedRows: 12,
        markerSkippedRows: 3,
      })),
    }));

    const { runCandidateRecordRolloverJob } = await import(
      "../../src/scheduler/candidateRecordScheduler.js"
    );

    const result = await runCandidateRecordRolloverJob({
      force: true,
      triggeredBy: "manual",
    });

    expect(result.forced).toBe(true);
    expect(result.force).toBe(true);
    expect(result.triggeredBy).toBe("manual");
    expect(result.emittedRows).toBe(12);
  });
});
