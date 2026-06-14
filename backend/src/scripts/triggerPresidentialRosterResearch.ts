import { enqueueManualPresidentialRosterResearchJob } from "../scheduler/presidentialRosterResearchScheduler.js";

async function main(): Promise<void> {
  const force = process.argv.includes("--force");
  const dryRun = process.argv.includes("--dry-run");
  const jobId = await enqueueManualPresidentialRosterResearchJob({ force, dryRun });
  console.log(
    `presidential roster research schedule job enqueued jobId=${jobId} force=${force} dryRun=${dryRun}`
  );
}

main().catch((error) => {
  console.error("presidential roster research schedule trigger failed:", error);
  process.exit(1);
});
