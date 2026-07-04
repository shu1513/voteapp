import { upsertRecurringElectionReminderJobs } from "../scheduler/electionReminderScheduler.js";

async function main(): Promise<void> {
  await upsertRecurringElectionReminderJobs();
  console.log("election_reminder daily scheduler upserted");
}

main().catch((error) => {
  console.error("election_reminder scheduler upsert failed:", error);
  process.exit(1);
});
