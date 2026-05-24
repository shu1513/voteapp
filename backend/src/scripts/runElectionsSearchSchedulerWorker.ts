import { createElectionsSearchSchedulerWorker } from "../scheduler/electionsSearchScheduler.js";

async function main(): Promise<void> {
  const worker = createElectionsSearchSchedulerWorker();

  worker.on("ready", () => {
    console.log("elections_search scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`elections_search scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(
      `elections_search scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`
    );
  });

  worker.on("failed", (job, error) => {
    console.error(`elections_search scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });

  const shutdown = async (): Promise<void> => {
    try {
      await worker.close();
      process.exit(0);
    } catch (error) {
      console.error("elections_search scheduler worker shutdown failed:", error);
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
  console.error("elections_search scheduler worker crashed:", error);
  process.exit(1);
});

