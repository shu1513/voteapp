import { enqueueManualElectionResultScheduleJob } from "../scheduler/electionResultScheduler.js";

async function main(): Promise<void> {
  const force = process.argv.includes("--force");
  const dryRun = process.argv.includes("--dry-run");
  const jobId = await enqueueManualElectionResultScheduleJob({ force, dryRun });
  console.log(`election_result schedule job enqueued jobId=${jobId} force=${force} dryRun=${dryRun}`);
}

main().catch((error) => {
  console.error("election_result schedule trigger failed:", error);
  process.exit(1);
});
