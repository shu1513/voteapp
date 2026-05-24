import { enqueueManualCandidateRosterRolloverJob } from "../scheduler/candidateRosterScheduler.js";

const force = process.argv.includes("--force");

enqueueManualCandidateRosterRolloverJob({ force })
  .then((jobId) => {
    console.log(`candidate_roster rollover job enqueued (jobId=${jobId} force=${force})`);
  })
  .catch((error) => {
    console.error("candidate_roster rollover trigger failed:", error);
    process.exit(1);
  });

