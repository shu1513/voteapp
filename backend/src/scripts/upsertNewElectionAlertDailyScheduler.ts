import { upsertRecurringNewElectionAlertJobs } from "../scheduler/newElectionAlertScheduler.js";

async function main(): Promise<void> {
  await upsertRecurringNewElectionAlertJobs();
  console.log("new_election_alert daily scheduler upserted");
}

main().catch((error) => {
  console.error("new_election_alert scheduler upsert failed:", error);
  process.exit(1);
});
