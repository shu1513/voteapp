import { enqueueManualElectionsSearchJob } from "../scheduler/electionsSearchScheduler.js";

const dryRun = process.argv.includes("--dry-run");
const force = process.argv.includes("--force");

enqueueManualElectionsSearchJob({ dryRun, force })
  .then((jobId) => {
    console.log(
      `elections_search rollover job enqueued (jobId=${jobId} dryRun=${dryRun} force=${force})`
    );
  })
  .catch((error) => {
    console.error("elections_search rollover trigger failed:", error);
    process.exit(1);
  });

