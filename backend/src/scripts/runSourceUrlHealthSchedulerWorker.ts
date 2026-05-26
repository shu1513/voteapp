import { createSourceUrlHealthSchedulerWorker } from "../scheduler/sourceUrlHealthScheduler.js";

async function main(): Promise<void> {
  const worker = createSourceUrlHealthSchedulerWorker();

  worker.on("ready", () => {
    console.log("source_url_health scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`source_url_health scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(
      `source_url_health scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`
    );
  });

  worker.on("failed", (job, error) => {
    console.error(`source_url_health scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });

  const shutdown = async (): Promise<void> => {
    try {
      await worker.close();
      process.exit(0);
    } catch (error) {
      console.error("source_url_health scheduler worker shutdown failed:", error);
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
  console.error("source_url_health scheduler worker crashed:", error);
  process.exitCode = 1;
});

