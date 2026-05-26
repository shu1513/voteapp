import { enqueueManualSourceUrlHealthJob } from "../scheduler/sourceUrlHealthScheduler.js";

const dryRun = process.argv.includes("--dry-run");
const force = process.argv.includes("--force");
// dry-run still performs HTTP checks for classification visibility;
// it only skips persistence/cleanup side effects.

enqueueManualSourceUrlHealthJob({ dryRun, force })
  .then((jobId) => {
    console.log(
      `source_url_health check job enqueued: jobId=${jobId} dryRun=${dryRun} force=${force}`
    );
  })
  .catch((error) => {
    console.error("source_url_health check trigger failed:", error);
    process.exitCode = 1;
  });
