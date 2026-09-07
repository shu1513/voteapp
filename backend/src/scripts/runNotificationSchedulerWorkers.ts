import { pathToFileURL } from "node:url";
import type { Worker } from "bullmq";

import { captureError, describeError, flushSentry, initSentryFromEnv } from "../observability/sentry.js";
import {
  createCandidateFollowDigestSchedulerWorker,
  upsertRecurringCandidateFollowDigestJobs,
} from "../scheduler/candidateFollowDigestScheduler.js";
import {
  createElectionReminderSchedulerWorker,
  upsertRecurringElectionReminderJobs,
} from "../scheduler/electionReminderScheduler.js";
import {
  createNewElectionAlertSchedulerWorker,
  upsertRecurringNewElectionAlertJobs,
} from "../scheduler/newElectionAlertScheduler.js";

/**
 * Single-process entrypoint for all three notification scheduler workers
 * (digest, new-election alerts, election reminders), sized for a deployment
 * that runs one background-worker instance instead of three.
 *
 * On boot it also upserts each recurring BullMQ job, replacing the separate
 * one-time `*:scheduler:upsert` deploy step — the upserts are idempotent, so
 * every restart just reasserts the same cron registrations.
 *
 * Job handlers take advisory locks (see the individual schedulers), so this
 * process can coexist with the standalone single-worker scripts without
 * double-sending.
 */

interface NamedWorkerFactory {
  readonly name: string;
  readonly upsert: () => Promise<void>;
  readonly create: () => Worker<never, unknown, string> | Worker;
}

const FACTORIES: readonly NamedWorkerFactory[] = [
  {
    name: "candidate_follow_digest",
    upsert: upsertRecurringCandidateFollowDigestJobs,
    create: createCandidateFollowDigestSchedulerWorker,
  },
  {
    name: "new_election_alert",
    upsert: upsertRecurringNewElectionAlertJobs,
    create: createNewElectionAlertSchedulerWorker,
  },
  {
    name: "election_reminder",
    upsert: upsertRecurringElectionReminderJobs,
    create: createElectionReminderSchedulerWorker,
  },
];

/** Number of per-user failures a completed job reported; 0 for any other result shape. */
export function countJobFailures(result: unknown): number {
  if (typeof result !== "object" || result === null) {
    return 0;
  }
  const failures = (result as { failures?: unknown }).failures;
  return Array.isArray(failures) ? failures.length : 0;
}

function wireWorkerEvents(workerName: string, worker: Worker<never, unknown, string> | Worker): void {
  worker.on("ready", () => {
    console.log(`${workerName} scheduler worker ready`);
  });
  worker.on("active", (job) => {
    console.log(`${workerName} scheduler worker active jobId=${job.id} name=${job.name}`);
  });
  worker.on("completed", (job, result) => {
    console.log(`${workerName} scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`);
    // The digest and new-election jobs complete even when some recipients
    // failed (their backlog persists and the next run retries), so a
    // "completed" event alone does not mean everyone was delivered to.
    // Surface those partial failures without changing the retry model.
    const failureCount = countJobFailures(result);
    if (failureCount > 0) {
      captureError(new Error(`${workerName} job completed with ${failureCount} per-user failure(s)`), {
        worker: workerName,
        event: "completed_with_failures",
        job_id: job.id ?? "unknown",
      });
    }
  });
  worker.on("failed", (job, error) => {
    console.error(`${workerName} scheduler worker failed jobId=${job?.id ?? "unknown"}:`, describeError(error));
    captureError(error, { worker: workerName, event: "failed", job_id: job?.id ?? "unknown" });
  });
  worker.on("error", (error) => {
    console.error(`${workerName} scheduler worker error:`, describeError(error));
    captureError(error, { worker: workerName, event: "error" });
  });
}

async function main(): Promise<void> {
  initSentryFromEnv("worker");

  for (const factory of FACTORIES) {
    await factory.upsert();
    console.log(`${factory.name} daily scheduler upserted`);
  }

  const workers = FACTORIES.map((factory) => {
    const worker = factory.create();
    wireWorkerEvents(factory.name, worker);
    return worker;
  });

  const shutdown = async (): Promise<void> => {
    try {
      await Promise.all(workers.map((worker) => worker.close()));
      await flushSentry();
      process.exit(0);
    } catch (error) {
      console.error("notification scheduler workers shutdown failed:", describeError(error));
      captureError(error, { worker: "notification_workers", event: "shutdown_failed" });
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
}

// Only boot when run directly: tests import countJobFailures and must not
// connect to Redis or start live consumers.
const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("notification scheduler workers crashed:", describeError(error));
    captureError(error, { worker: "notification_workers", event: "crashed" });
    void flushSentry().finally(() => {
      process.exit(1);
    });
  });
}
