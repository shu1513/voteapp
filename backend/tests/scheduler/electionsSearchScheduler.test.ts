import { describe, expect, it, vi } from "vitest";

describe("runElectionsSearchRolloverJob", () => {
  it("passes dryRun/force through and preserves triggeredBy", async () => {
    vi.resetModules();
    vi.doMock("../../src/pipeline/producers/electionsSearchRolloverProducer.js", () => ({
      runElectionsSearchRolloverProducer: vi.fn(async (input: { dryRun?: boolean; force?: boolean }) => ({
        enabled: true,
        asOfDate: "2026-05-23",
        cooldownDays: 180,
        maxEnqueuePerRun: 5000,
        districts_scanned: 100,
        due_count: 20,
        due_overflow_count: 0,
        enqueued_count: 10,
        skipped_cooldown: 70,
        skipped_not_due: 10,
        max_enqueue_hit: false,
        failed_count: 0,
        dryRun: Boolean(input?.dryRun),
        force: Boolean(input?.force),
      })),
    }));

    const { runElectionsSearchRolloverJob } = await import(
      "../../src/scheduler/electionsSearchScheduler.js"
    );

    const result = await runElectionsSearchRolloverJob({
      dryRun: true,
      force: true,
      triggeredBy: "manual",
    });

    expect(result.dryRun).toBe(true);
    expect(result.force).toBe(true);
    expect(result.triggeredBy).toBe("manual");
    expect(result.enqueued_count).toBe(10);
  });
});
