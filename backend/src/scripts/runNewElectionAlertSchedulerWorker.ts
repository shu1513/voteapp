import { createNewElectionAlertSchedulerWorker } from "../scheduler/newElectionAlertScheduler.js";
import { captureError, flushSentry, initSentryFromEnv } from "../observability/sentry.js";

async function main(): Promise<void> {
  initSentryFromEnv("worker");
  const worker = createNewElectionAlertSchedulerWorker();

  worker.on("ready", () => {
    console.log("new_election_alert scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`new_election_alert scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(`new_election_alert scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`);
  });

  worker.on("failed", (job, error) => {
    console.error(`new_election_alert scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
    captureError(error, { worker: "new_election_alert", event: "failed", job_id: job?.id ?? "unknown" });
  });

  worker.on("error", (error) => {
    console.error("new_election_alert scheduler worker error:", error);
    captureError(error, { worker: "new_election_alert", event: "error" });
  });

  const shutdown = async (): Promise<void> => {
    try {
      await worker.close();
      process.exit(0);
    } catch (error) {
      console.error("new_election_alert scheduler worker shutdown failed:", error);
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

main().catch((error) => {
  console.error(
    "new_election_alert scheduler worker crashed:",
    error instanceof Error ? (error.stack ?? error.message) : String(error)
  );
  captureError(error, { worker: "new_election_alert", event: "crashed" });
  void flushSentry().finally(() => {
    process.exit(1);
  });
});
