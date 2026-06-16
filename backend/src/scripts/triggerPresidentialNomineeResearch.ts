import { enqueueManualPresidentialNomineeResearchJob } from "../scheduler/presidentialNomineeResearchScheduler.js";

async function main(): Promise<void> {
  const force = process.argv.includes("--force");
  const dryRun = process.argv.includes("--dry-run");
  const jobId = await enqueueManualPresidentialNomineeResearchJob({ force, dryRun });
  if (jobId === "disabled") {
    console.log(
      `presidential nominee research schedule disabled; no job enqueued force=${force} dryRun=${dryRun}`
    );
    return;
  }
  console.log(
    `presidential nominee research schedule job enqueued jobId=${jobId} force=${force} dryRun=${dryRun}`
  );
}

main().catch((error) => {
  console.error("presidential nominee research schedule trigger failed:", error);
  process.exit(1);
});
