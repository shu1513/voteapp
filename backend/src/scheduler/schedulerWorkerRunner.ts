import type { Worker } from "bullmq";

import { captureError, flushSentry, initSentryFromEnv } from "../observability/sentry.js";

/**
 * Shared entrypoint body for the notification scheduler workers: identical
 * logging, Sentry capture on failed/error/crash, and a shutdown that flushes
 * pending events before exiting — SIGTERM arrives on every rolling deploy,
 * and events captured by a just-failed job would otherwise be dropped.
 */
export function runSchedulerWorker(
  workerName: string,
  createWorker: () => Worker<never, unknown, string> | Worker
): void {
  try {
    initSentryFromEnv("worker");
    const worker = createWorker();

    worker.on("ready", () => {
      console.log(`${workerName} scheduler worker ready`);
    });

    worker.on("active", (job) => {
      console.log(`${workerName} scheduler worker active jobId=${job.id} name=${job.name}`);
    });

    worker.on("completed", (job, result) => {
      console.log(`${workerName} scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`);
    });

    worker.on("failed", (job, error) => {
      console.error(`${workerName} scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
      captureError(error, { worker: workerName, event: "failed", job_id: job?.id ?? "unknown" });
    });

    worker.on("error", (error) => {
      console.error(`${workerName} scheduler worker error:`, error);
      captureError(error, { worker: workerName, event: "error" });
    });

    const shutdown = async (): Promise<void> => {
      try {
        await worker.close();
        await flushSentry();
        process.exit(0);
      } catch (error) {
        console.error(`${workerName} scheduler worker shutdown failed:`, error);
        captureError(error, { worker: workerName, event: "shutdown_failed" });
        await flushSentry();
        process.exit(1);
      }
    };

    process.on("SIGINT", () => {
      void shutdown();
    });
    process.on("SIGTERM", () => {
      void shutdown();
    });
  } catch (error) {
    console.error(
      `${workerName} scheduler worker crashed:`,
      error instanceof Error ? (error.stack ?? error.message) : String(error)
    );
    captureError(error, { worker: workerName, event: "crashed" });
    void flushSentry().finally(() => {
      process.exit(1);
    });
  }
}
