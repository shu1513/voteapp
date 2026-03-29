import { createStateResourcesSchedulerWorker } from "../scheduler/stateResourcesScheduler.js";

async function main(): Promise<void> {
  const worker = createStateResourcesSchedulerWorker();

  worker.on("ready", () => {
    console.log("state_resources scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`state_resources scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(
      `state_resources scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`
    );
  });

  worker.on("failed", (job, error) => {
    console.error(
      `state_resources scheduler worker failed jobId=${job?.id ?? "unknown"}:`,
      error
    );
  });

  const shutdown = async (): Promise<void> => {
    try {
      await worker.close();
    } finally {
      process.exit(0);
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
  console.error("state_resources scheduler worker crashed:", error);
  process.exit(1);
});
