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
      failedSample: [],
    });
  });

  it("reads the attempted/succeeded/failed shape", () => {
    expect(summarizeFinanceRunResult({ attempted: 5, succeeded: 4, failed: 1, dryRun: true })).toEqual({
      line: "attempted=5 succeeded=4 failed=1 dryRun=true",
      failureCount: 1,
      failedSample: [],
    });
  });

  it("counts a failed outside-spending year as degraded even when every candidate synced", () => {
    // Missouri shape
    expect(
      summarizeFinanceRunResult({
        selectedCandidateCount: 4,
        syncedCandidateCount: 4,
        failedCandidateCount: 0,
        outsideArtifactYearCount: 1,
        failedOutsideArtifactYearCount: 1,
        dryRun: false,
      })
    ).toMatchObject({ line: "selected=4 synced=4 failed=0 outsideYears=1 outsideYearsFailed=1 dryRun=false", failureCount: 1 });
    // Montana shape
    expect(
      summarizeFinanceRunResult({ attempted: 2, succeeded: 2, failed: 0, outsideSweepYearCount: 0, failedOutsideSweepYearCount: 2 })
    ).toMatchObject({ line: "attempted=2 succeeded=2 failed=0 outsideYears=0 outsideYearsFailed=2", failureCount: 2 });
  });

  it("keeps a bounded, scrubbed sample of failed items from results or candidates", () => {
    const results = Array.from({ length: 7 }, (_, i) => ({
      candidateId: `cand-${i}`,
      ok: i === 1,
      error: i === 1 ? undefined : `OpenFEC 429 https://api.open.fec.gov/v1/x?api_key=SECRET&page=${i}`,
    }));
    const summary = summarizeFinanceRunResult({ selectedCandidateCount: 7, syncedCandidateCount: 1, failedCandidateCount: 6, results });
    expect(summary.failedSample).toHaveLength(5);
    expect(summary.failedSample[0]).toBe("cand-0: OpenFEC 429 https://api.open.fec.gov/v1/x?[scrubbed]");
    expect(summary.failedSample.join(" ")).not.toContain("SECRET");
    expect(summary.failedSample.some((entry) => entry.startsWith("cand-1:"))).toBe(false);

    // Montana nests the id under row and uses `candidates`
    expect(
      summarizeFinanceRunResult({
        attempted: 1,
        succeeded: 0,
        failed: 1,
        candidates: [{ row: { candidateId: "mt-1" }, ok: false, error: "timeout" }],
      }).failedSample
    ).toEqual(["mt-1: timeout"]);
  });

  it("never counts an unknown shape as degraded", () => {
    expect(summarizeFinanceRunResult({ something: "else" })).toEqual({
      line: 'result={"something":"else"}',
      failureCount: 0,
      failedSample: [],
    });
    expect(summarizeFinanceRunResult(undefined)).toEqual({ line: "result=undefined", failureCount: 0, failedSample: [] });
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

    worker.emit(
      "completed",
      { id: "j2" },
      {
        selectedCandidateCount: 3,
        syncedCandidateCount: 1,
        failedCandidateCount: 2,
        dryRun: false,
        results: [
          { candidateId: "a", ok: true },
          { candidateId: "b", ok: false, error: "HTTP 500" },
          { candidateId: "c", ok: false, error: "parse failed for contact@example.org" },
        ],
      }
    );

    expect(d.capture).toHaveBeenCalledTimes(1);
    const [captured, tags] = d.capture.mock.calls[0]!;
    expect((captured as Error).message).toBe(
      "Zetaland campaign finance sync scheduler worker completed DEGRADED jobId=j2 selected=3 synced=1 failed=2 dryRun=false failedSample=[b: HTTP 500 | c: parse failed for [email]]"
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
