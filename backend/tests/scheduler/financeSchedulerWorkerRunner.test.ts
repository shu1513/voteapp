import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";

import {
  attachFinanceWorkerReporting,
  summarizeFinanceRunResult,
  type FinanceWorkerLike,
} from "../../src/scheduler/financeSchedulerWorkerRunner.js";

function fakeWorker(): FinanceWorkerLike & EventEmitter {
  const emitter = new EventEmitter() as FinanceWorkerLike & EventEmitter;
  (emitter as { close?: () => Promise<void> }).close = async () => undefined;
  return emitter;
}

function deps() {
  return { log: vi.fn(), error: vi.fn(), capture: vi.fn() };
}

describe("summarizeFinanceRunResult", () => {
  it("reads the selected/synced/failed shape, with optional auto-link and flags", () => {
    expect(
      summarizeFinanceRunResult({
        selectedCandidateCount: 10,
        syncedCandidateCount: 8,
        failedCandidateCount: 2,
        autoLinkAttemptedCount: 3,
        autoLinkLinkedCount: 2,
        autoLinkFailedCount: 1,
        dryRun: false,
        includeOutside: true,
        dataSource: { mode: "bulk" },
        totalDueRows: 40,
      })
    ).toEqual({
      line: "selected=10 synced=8 failed=2 autoLinkAttempted=3 autoLinkLinked=2 autoLinkFailed=1 totalDueRows=40 dryRun=false includeOutside=true dataSource=bulk",
      failureCount: 3,
    });
  });

  it("reads the attempted/succeeded/failed shape", () => {
    expect(summarizeFinanceRunResult({ attempted: 5, succeeded: 4, failed: 1, dryRun: true })).toEqual({
      line: "attempted=5 succeeded=4 failed=1 dryRun=true",
      failureCount: 1,
    });
  });

  it("never counts an unknown shape as degraded", () => {
    expect(summarizeFinanceRunResult({ something: "else" })).toEqual({ line: 'result={"something":"else"}', failureCount: 0 });
    expect(summarizeFinanceRunResult(undefined)).toEqual({ line: "result=undefined", failureCount: 0 });
  });
});

describe("attachFinanceWorkerReporting", () => {
  it("logs a clean completion without capturing", () => {
    const worker = fakeWorker();
    const d = deps();
    attachFinanceWorkerReporting(worker, "Zetaland campaign finance sync", d);

    worker.emit("ready");
    worker.emit("active", { id: "j1", name: "daily" });
    worker.emit("completed", { id: "j1" }, { selectedCandidateCount: 3, syncedCandidateCount: 3, failedCandidateCount: 0, dryRun: false });

    expect(d.log).toHaveBeenCalledWith("Zetaland campaign finance sync scheduler worker ready");
    expect(d.log).toHaveBeenCalledWith("Zetaland campaign finance sync scheduler worker active jobId=j1 name=daily");
    expect(d.log).toHaveBeenCalledWith(
      "Zetaland campaign finance sync scheduler worker completed jobId=j1 selected=3 synced=3 failed=0 dryRun=false"
    );
    expect(d.capture).not.toHaveBeenCalled();
    expect(d.error).not.toHaveBeenCalled();
  });

  it("captures a completed run that left candidates failed", () => {
    const worker = fakeWorker();
    const d = deps();
    attachFinanceWorkerReporting(worker, "Zetaland campaign finance sync", d);

    worker.emit("completed", { id: "j2" }, { selectedCandidateCount: 3, syncedCandidateCount: 1, failedCandidateCount: 2, dryRun: false });

    expect(d.capture).toHaveBeenCalledTimes(1);
    const [captured, tags] = d.capture.mock.calls[0]!;
    expect((captured as Error).message).toBe(
      "Zetaland campaign finance sync scheduler worker completed DEGRADED jobId=j2 selected=3 synced=1 failed=2 dryRun=false"
    );
    expect(tags).toEqual({ worker: "Zetaland campaign finance sync", event: "degraded", job_id: "j2" });
    expect(d.log).not.toHaveBeenCalled();
  });

  it("captures an auto-link-only failure and the attempted/failed shape", () => {
    const worker = fakeWorker();
    const d = deps();
    attachFinanceWorkerReporting(worker, "Vermont campaign finance sync", d);

    worker.emit("completed", { id: "j3" }, { selectedCandidateCount: 2, syncedCandidateCount: 2, failedCandidateCount: 0, autoLinkAttemptedCount: 1, autoLinkLinkedCount: 0, autoLinkFailedCount: 1, dryRun: false });
    worker.emit("completed", { id: "j4" }, { attempted: 4, succeeded: 3, failed: 1, dryRun: false });

    expect(d.capture).toHaveBeenCalledTimes(2);
    expect(d.capture.mock.calls[0]?.[1]).toMatchObject({ event: "degraded", job_id: "j3" });
    expect(d.capture.mock.calls[1]?.[1]).toMatchObject({ event: "degraded", job_id: "j4" });
  });

  it("captures failed and error events with the worker tag", () => {
    const worker = fakeWorker();
    const d = deps();
    attachFinanceWorkerReporting(worker, "Zetaland campaign finance sync", d);
    const boom = new Error("redis gone");

    worker.emit("failed", { id: "j5" }, boom);
    worker.emit("failed", undefined, boom);
    worker.emit("error", boom);

    expect(d.capture).toHaveBeenNthCalledWith(1, boom, { worker: "Zetaland campaign finance sync", event: "failed", job_id: "j5" });
    expect(d.capture).toHaveBeenNthCalledWith(2, boom, { worker: "Zetaland campaign finance sync", event: "failed", job_id: "unknown" });
    expect(d.capture).toHaveBeenNthCalledWith(3, boom, { worker: "Zetaland campaign finance sync", event: "error" });
    expect(d.error).toHaveBeenCalledTimes(3);
  });
});
