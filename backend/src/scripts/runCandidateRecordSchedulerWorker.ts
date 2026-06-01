import { createCandidateRecordSchedulerWorker } from "../scheduler/candidateRecordScheduler.js";

async function main(): Promise<void> {
  const worker = createCandidateRecordSchedulerWorker();

  worker.on("ready", () => {
    console.log("candidate_record scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`candidate_record scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(
      `candidate_record scheduler worker completed jobId=${job.id} result=${JSON.stringify(result)}`
    );
  });

  worker.on("failed", (job, error) => {
    console.error(`candidate_record scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });

  worker.on("error", (error) => {
    console.error("candidate_record scheduler worker error:", error);
  });

  const shutdown = async (): Promise<void> => {
    try {
      await worker.close();
      process.exit(0);
    } catch (error) {
      console.error("candidate_record scheduler worker shutdown failed:", error);
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
  console.error("candidate_record scheduler worker crashed:", error);
  process.exit(1);
});
