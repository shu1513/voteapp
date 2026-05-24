import { createCandidateRosterSchedulerWorker } from "../scheduler/candidateRosterScheduler.js";

async function main(): Promise<void> {
  const worker = createCandidateRosterSchedulerWorker();

  worker.on("ready", () => {
    console.log("candidate_roster scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`candidate_roster scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(
      `candidate_roster scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`
    );
  });

  worker.on("failed", (job, error) => {
    console.error(
      `candidate_roster scheduler worker failed jobId=${job?.id ?? "unknown"}:`,
      error
    );
  });

  const shutdown = async (): Promise<void> => {
    try {
      await worker.close();
      process.exit(0);
    } catch (error) {
      console.error("candidate_roster scheduler worker shutdown failed:", error);
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
  console.error("candidate_roster scheduler worker crashed:", error);
  process.exit(1);
});
