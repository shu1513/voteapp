import { upsertRecurringStateResourcesRefreshJobs } from "../scheduler/stateResourcesScheduler.js";

const dryRun = process.argv.includes("--dry-run");
const force = process.argv.includes("--force");

upsertRecurringStateResourcesRefreshJobs({ dryRun, force })
  .then(() => {
    console.log(
      `state_resources recurring scheduler upserted (monthly) (dryRun=${dryRun} force=${force})`
    );
  })
  .catch((error) => {
    console.error("state_resources recurring scheduler upsert failed:", error);
    process.exit(1);
  });
