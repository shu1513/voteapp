import { upsertRecurringCandidateRosterRolloverJobs } from "../scheduler/candidateRosterScheduler.js";

upsertRecurringCandidateRosterRolloverJobs()
  .then(() => {
    console.log("candidate_roster recurring scheduler upserted (daily rollover)");
  })
  .catch((error) => {
    console.error("candidate_roster recurring scheduler upsert failed:", error);
    process.exit(1);
  });

