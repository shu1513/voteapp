import { loadProjectEnv } from "../config/env.js";
import { isOregonCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createOregonCandidateFinanceSyncSchedulerWorker } from "../scheduler/oregonCandidateFinanceSyncScheduler.js";

const SHUTDOWN_TIMEOUT_MS = 30_000;

async function main(): Promise<void> {
  loadProjectEnv();
  if (!isOregonCampaignFinanceEnabled()) {
    console.log("Oregon campaign finance sync scheduler worker disabled; exiting");
    return;
  }

  const worker = createOregonCandidateFinanceSyncSchedulerWorker();
  let shutdownPromise: Promise<void> | null = null;

  worker.on("ready", () => {
    console.log("Oregon campaign finance sync scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`Oregon campaign finance sync scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(
      "Oregon campaign finance sync scheduler worker completed " +
        `jobId=${job.id} selected=${result.selectedCandidateCount} synced=${result.syncedCandidateCount} ` +
        `failed=${result.failedCandidateCount} dryRun=${result.dryRun}`
    );
  });

  worker.on("failed", (job, error) => {
    console.error(`Oregon campaign finance sync scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });

  worker.on("error", (error) => {
    console.error("Oregon campaign finance sync scheduler worker error:", error);
  });

  const shutdown = (): Promise<void> => {
    if (shutdownPromise) {
      return shutdownPromise;
    }

    shutdownPromise = new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        console.error("Oregon campaign finance sync scheduler worker shutdown timed out");
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
            console.error("Oregon campaign finance sync scheduler worker shutdown failed:", error);
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
  console.error("Oregon campaign finance sync scheduler worker crashed:", error);
  process.exit(1);
});
