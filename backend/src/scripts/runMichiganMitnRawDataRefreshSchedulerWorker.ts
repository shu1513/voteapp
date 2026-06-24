import { loadProjectEnv } from "../config/env.js";
import { isMichiganCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createMichiganMitnRawDataRefreshWorker } from "../scheduler/michiganMitnRawDataRefreshScheduler.js";

const SHUTDOWN_TIMEOUT_MS = 30_000;

async function main(): Promise<void> {
  loadProjectEnv();
  if (!isMichiganCampaignFinanceEnabled()) {
    console.log("Michigan MiTN raw-data refresh scheduler worker disabled; exiting");
    return;
  }

  const worker = createMichiganMitnRawDataRefreshWorker();
  let shutdownPromise: Promise<void> | null = null;

  worker.on("ready", () => {
    console.log("Michigan MiTN raw-data refresh scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`Michigan MiTN raw-data refresh scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(`Michigan MiTN raw-data refresh scheduler worker completed jobId=${job.id} status=${result.status}`);
  });

  worker.on("failed", (job, error) => {
    console.error(`Michigan MiTN raw-data refresh scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });

  worker.on("error", (error) => {
    console.error("Michigan MiTN raw-data refresh scheduler worker error:", error);
  });

  const shutdown = (): Promise<void> => {
    if (shutdownPromise) {
      return shutdownPromise;
    }

    shutdownPromise = new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        console.error("Michigan MiTN raw-data refresh scheduler worker shutdown timed out");
        process.exit(1);
      }, SHUTDOWN_TIMEOUT_MS);
      timeout.unref();

      void worker
        .close()
        .then(
          () => {
            clearTimeout(timeout);
            process.exit(0);
          },
          (error) => {
            clearTimeout(timeout);
            console.error("Michigan MiTN raw-data refresh scheduler worker shutdown failed:", error);
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
}

main().catch((error) => {
  console.error("Michigan MiTN raw-data refresh scheduler worker crashed:", error);
  process.exit(1);
});
