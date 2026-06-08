import { upsertRecurringElectionResultScheduleJobs } from "../scheduler/electionResultScheduler.js";

async function main(): Promise<void> {
  await upsertRecurringElectionResultScheduleJobs();
  console.log("election_result daily scheduler upserted");
}

main().catch((error) => {
  console.error("election_result scheduler upsert failed:", error);
  process.exit(1);
});
