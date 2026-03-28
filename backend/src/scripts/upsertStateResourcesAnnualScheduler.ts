import { upsertAnnualStateResourcesRefreshJob } from "../scheduler/stateResourcesScheduler.js";

const dryRun = process.argv.includes("--dry-run");
const force = process.argv.includes("--force");

upsertAnnualStateResourcesRefreshJob({ dryRun, force })
  .then(() => {
    console.log(
      `state_resources annual scheduler upserted (dryRun=${dryRun} force=${force})`
    );
  })
  .catch((error) => {
    console.error("state_resources annual scheduler upsert failed:", error);
    process.exit(1);
  });
