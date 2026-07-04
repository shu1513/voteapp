import { createElectionReminderSchedulerWorker } from "../scheduler/electionReminderScheduler.js";

async function main(): Promise<void> {
  const worker = createElectionReminderSchedulerWorker();

  worker.on("ready", () => {
    console.log("election_reminder scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`election_reminder scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(`election_reminder scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`);
  });

  worker.on("failed", (job, error) => {
    console.error(`election_reminder scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });

  worker.on("error", (error) => {
    console.error("election_reminder scheduler worker error:", error);
  });

  const shutdown = async (): Promise<void> => {
    try {
      await worker.close();
      process.exit(0);
    } catch (error) {
      console.error("election_reminder scheduler worker shutdown failed:", error);
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
  console.error("election_reminder scheduler worker crashed:", error);
  process.exit(1);
});
