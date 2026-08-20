import { loadProjectEnv } from "../config/env.js";
import { isMissouriCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createMissouriCandidateFinanceSyncSchedulerWorker } from "../scheduler/missouriCandidateFinanceSyncScheduler.js";

async function main(): Promise<void> {
  loadProjectEnv();
  if (!isMissouriCampaignFinanceEnabled()) {
    console.log("Missouri campaign finance sync scheduler worker disabled; exiting");
    return;
  }
  const worker = createMissouriCandidateFinanceSyncSchedulerWorker();
  worker.on("ready", () => console.log("Missouri campaign finance sync scheduler worker ready"));
  worker.on("completed", (job, result) => console.log(`Missouri finance sync completed jobId=${job.id} synced=${result.syncedCandidateCount} failed=${result.failedCandidateCount}`));
  worker.on("failed", (job, error) => console.error(`Missouri finance sync failed jobId=${job?.id ?? "unknown"}:`, error));
  worker.on("error", (error) => console.error("Missouri finance worker error:", error));
  let closing = false;
  const close = async () => {
    if (closing) return;
    closing = true;
    await worker.close();
  };
  process.on("SIGINT", () => void close());
  process.on("SIGTERM", () => void close());
}

main().catch((error) => { console.error("Missouri finance worker crashed:", error); process.exit(1); });
