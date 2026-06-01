import { upsertRecurringCandidateRecordRolloverJobs } from "../scheduler/candidateRecordScheduler.js";

async function main(): Promise<void> {
  await upsertRecurringCandidateRecordRolloverJobs();
  console.log("candidate_record daily scheduler upserted");
}

main().catch((error) => {
  console.error("candidate_record scheduler upsert failed:", error);
  process.exit(1);
});
