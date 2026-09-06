import { loadProjectEnv } from "../config/env.js";
import { captureError, describeError, flushSentry, initSentryFromEnv } from "../observability/sentry.js";

/**
 * Shared entrypoint body for the campaign-finance scheduler workers.
 *
 * Every finance worker script used to repeat the same eighty lines — flag
 * gate, ready/active/completed/failed/error logging, bounded shutdown — and
 * none of them initialized Sentry or captured anything, so a run that failed
 * every candidate, or a worker that kept erroring, was invisible outside
 * the process log. This runner owns that lifecycle once and reports:
 *
 * - `failed` / `error` events (captured, like the notification workers);
 * - DEGRADED completions: a job that finished but left candidates failed —
 *   `failedCandidateCount`, an auto-link-only `autoLinkFailedCount`, or the
 *   attempted/succeeded/failed shape some states use. Those rows keep a
 *   stale `last_synced_at` and are retried by the next due-list run, but a
 *   persistent failure must not stay silent.
 *
 * Feature gates stay per worker (`isEnabled`); shutdown stays bounded so a
 * stuck close cannot block a rolling deploy.
 */

const DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;

/** The subset of a BullMQ Worker the runner needs (structural, so tests can
 * pass an EventEmitter). */
export type FinanceWorkerLike = {
  on(event: "ready", listener: () => void): unknown;
  on(event: "active", listener: (job: { id?: string | null; name?: string }) => void): unknown;
  on(event: "completed", listener: (job: { id?: string | null }, result: unknown) => void): unknown;
  on(event: "failed", listener: (job: { id?: string | null } | undefined, error: unknown) => void): unknown;
  on(event: "error", listener: (error: unknown) => void): unknown;
  close(): Promise<void>;
};

export type FinanceRunSummary = {
  /** One-line human summary of the known count fields. */
  line: string;
  /** Candidates (or auto-link attempts) the run could not finish. */
  failureCount: number;
};

function readCount(record: Record<string, unknown>, key: string): number | undefined {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

/**
 * Reads the count fields the state schedulers agree on. Two shapes exist:
 * selected/synced/failed candidate counts (most states, optionally with
 * auto-link counts) and attempted/succeeded/failed (Montana, South
 * Carolina). Unknown shapes are logged as JSON and never counted as
 * degraded — a missing field is not a failure.
 */
export function summarizeFinanceRunResult(result: unknown): FinanceRunSummary {
  if (result === null || typeof result !== "object") {
    return { line: `result=${JSON.stringify(result)}`, failureCount: 0 };
  }
  const record = result as Record<string, unknown>;
  const parts: string[] = [];
  const push = (label: string, value: number | undefined) => {
    if (value !== undefined) parts.push(`${label}=${value}`);
  };
  const failedCandidates = readCount(record, "failedCandidateCount");
  const autoLinkFailed = readCount(record, "autoLinkFailedCount");
  const failedAttempts = readCount(record, "failed");

  push("selected", readCount(record, "selectedCandidateCount"));
  push("synced", readCount(record, "syncedCandidateCount"));
  push("skipped", readCount(record, "skippedCandidateCount"));
  push("failed", failedCandidates);
  push("attempted", readCount(record, "attempted"));
  push("succeeded", readCount(record, "succeeded"));
  if (failedCandidates === undefined) push("failed", failedAttempts);
  push("autoLinkAttempted", readCount(record, "autoLinkAttemptedCount"));
  push("autoLinkLinked", readCount(record, "autoLinkLinkedCount"));
  push("autoLinkFailed", autoLinkFailed);
  push("totalDueRows", readCount(record, "totalDueRows"));
  if (typeof record.dryRun === "boolean") parts.push(`dryRun=${record.dryRun}`);
  if (typeof record.includeOutside === "boolean") parts.push(`includeOutside=${record.includeOutside}`);
  const dataSource = record.dataSource;
  if (dataSource && typeof dataSource === "object" && typeof (dataSource as { mode?: unknown }).mode === "string") {
    parts.push(`dataSource=${(dataSource as { mode: string }).mode}`);
  }

  const failureCount = (failedCandidates ?? failedAttempts ?? 0) + (autoLinkFailed ?? 0);
  return {
    line: parts.length > 0 ? parts.join(" ") : `result=${JSON.stringify(result)}`,
    failureCount,
  };
}

export type FinanceWorkerReportingDeps = {
  log: (message: string) => void;
  error: (message: string, error?: unknown) => void;
  capture: (error: unknown, tags: Record<string, string>) => void;
};

const defaultDeps: FinanceWorkerReportingDeps = {
  log: (message) => console.log(message),
  error: (message, error) => (error === undefined ? console.error(message) : console.error(message, describeError(error))),
  capture: captureError,
};

/** Attaches the shared handlers. Exported for tests; runFinanceSchedulerWorker wires it. */
export function attachFinanceWorkerReporting(
  worker: FinanceWorkerLike,
  label: string,
  deps: FinanceWorkerReportingDeps = defaultDeps
): void {
  const prefix = `${label} scheduler worker`;
  worker.on("ready", () => {
    deps.log(`${prefix} ready`);
  });
  worker.on("active", (job) => {
    deps.log(`${prefix} active jobId=${job.id ?? "unknown"} name=${job.name ?? "unknown"}`);
  });
  worker.on("completed", (job, result) => {
    const summary = summarizeFinanceRunResult(result);
    const jobId = job.id ?? "unknown";
    if (summary.failureCount > 0) {
      const message = `${prefix} completed DEGRADED jobId=${jobId} ${summary.line}`;
      deps.error(message);
      deps.capture(new Error(message), { worker: label, event: "degraded", job_id: jobId });
      return;
    }
    deps.log(`${prefix} completed jobId=${jobId} ${summary.line}`);
  });
  worker.on("failed", (job, error) => {
    const jobId = job?.id ?? "unknown";
    deps.error(`${prefix} failed jobId=${jobId}:`, error);
    deps.capture(error, { worker: label, event: "failed", job_id: jobId });
  });
  worker.on("error", (error) => {
    deps.error(`${prefix} error:`, error);
    deps.capture(error, { worker: label, event: "error" });
  });
}

export type RunFinanceSchedulerWorkerInput = {
  /** Log prefix, e.g. "New Mexico campaign finance sync". */
  label: string;
  /** Feature gate; a disabled worker logs once and exits 0 without connecting. */
  isEnabled: () => boolean;
  createWorker: () => FinanceWorkerLike;
  shutdownTimeoutMs?: number;
};

export function runFinanceSchedulerWorker(input: RunFinanceSchedulerWorkerInput): void {
  const prefix = `${input.label} scheduler worker`;
  loadProjectEnv();
  if (!input.isEnabled()) {
    console.log(`${prefix} disabled; exiting`);
    return;
  }

  try {
    initSentryFromEnv("worker");
    const worker = input.createWorker();
    attachFinanceWorkerReporting(worker, input.label);

    let shutdownPromise: Promise<void> | null = null;
    const shutdown = (): Promise<void> => {
      if (shutdownPromise) {
        return shutdownPromise;
      }
      shutdownPromise = new Promise<void>((resolve) => {
        const timeout = setTimeout(() => {
          console.error(`${prefix} shutdown timed out`);
          void flushSentry().finally(() => process.exit(1));
        }, input.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS);
        timeout.unref();

        void worker
          .close()
          .then(
            async () => {
              clearTimeout(timeout);
              await flushSentry();
              process.exit(0);
            },
            async (error) => {
              clearTimeout(timeout);
              console.error(`${prefix} shutdown failed:`, describeError(error));
              captureError(error, { worker: input.label, event: "shutdown_failed" });
              await flushSentry();
              process.exit(1);
            }
          )
          .finally(resolve);
      });
      return shutdownPromise;
    };

    process.on("SIGINT", () => {
      void shutdown();
    });
    process.on("SIGTERM", () => {
      void shutdown();
    });
  } catch (error) {
    console.error(`${prefix} crashed:`, describeError(error));
    captureError(error, { worker: input.label, event: "crashed" });
    void flushSentry().finally(() => {
      process.exit(1);
    });
  }
}
