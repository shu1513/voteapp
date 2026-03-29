import { enqueueManualStateResourcesRefreshJob } from "../scheduler/stateResourcesScheduler.js";

const dryRun = process.argv.includes("--dry-run");
const force = process.argv.includes("--force");

enqueueManualStateResourcesRefreshJob({ dryRun, force })
  .then((jobId) => {
    console.log(
      `state_resources refresh job enqueued (jobId=${jobId} dryRun=${dryRun} force=${force})`
    );
  })
  .catch((error) => {
    console.error("state_resources refresh trigger failed:", error);
    process.exit(1);
  });
