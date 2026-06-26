import { loadProjectEnv } from "../config/env.js";
import { isPennsylvaniaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { createPennsylvaniaCandidateFinanceSyncSchedulerWorker } from "../scheduler/pennsylvaniaCandidateFinanceSyncScheduler.js";

const SHUTDOWN_TIMEOUT_MS = 30_000;

async function main(): Promise<void> {
  loadProjectEnv();
  if (!isPennsylvaniaCampaignFinanceSyncEnabled(false)) {
    console.log("Pennsylvania campaign finance sync scheduler worker disabled; exiting");
    return;
  }

  const worker = createPennsylvaniaCandidateFinanceSyncSchedulerWorker();
  let shutdownPromise: Promise<void> | null = null;

  worker.on("ready", () => {
    console.log("Pennsylvania campaign finance sync scheduler worker ready");
  });

  worker.on("active", (job) => {
    console.log(`Pennsylvania campaign finance sync scheduler worker active jobId=${job.id} name=${job.name}`);
  });

  worker.on("completed", (job, result) => {
    console.log(
      "Pennsylvania campaign finance sync scheduler worker completed " +
        `jobId=${job.id} selected=${result.selectedCandidateCount} synced=${result.syncedCandidateCount} ` +
        `failed=${result.failedCandidateCount} dryRun=${result.dryRun}`
    );
  });

  worker.on("failed", (job, error) => {
    console.error(`Pennsylvania campaign finance sync scheduler worker failed jobId=${job?.id ?? "unknown"}:`, error);
  });

  worker.on("error", (error) => {
    console.error("Pennsylvania campaign finance sync scheduler worker error:", error);
  });

  const shutdown = (): Promise<void> => {
    if (shutdownPromise) {
      return shutdownPromise;
    }

    shutdownPromise = new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        console.error("Pennsylvania campaign finance sync scheduler worker shutdown timed out");
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
            console.error("Pennsylvania campaign finance sync scheduler worker shutdown failed:", error);
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
  console.error("Pennsylvania campaign finance sync scheduler worker crashed:", error);
  process.exit(1);
});
