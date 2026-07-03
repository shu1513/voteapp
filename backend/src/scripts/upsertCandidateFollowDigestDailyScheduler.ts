import { upsertRecurringCandidateFollowDigestJobs } from "../scheduler/candidateFollowDigestScheduler.js";

async function main(): Promise<void> {
  await upsertRecurringCandidateFollowDigestJobs();
  console.log("candidate_follow_digest daily scheduler upserted");
}

main().catch((error) => {
  console.error("candidate_follow_digest scheduler upsert failed:", error);
  process.exit(1);
});
