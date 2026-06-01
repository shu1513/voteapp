import { enqueueManualCandidateRecordRolloverJob } from "../scheduler/candidateRecordScheduler.js";

async function main(): Promise<void> {
  const force = process.argv.includes("--force");
  const jobId = await enqueueManualCandidateRecordRolloverJob({ force });
  console.log(`candidate_record rollover job enqueued jobId=${jobId} force=${force}`);
}

main().catch((error) => {
  console.error("candidate_record rollover trigger failed:", error);
  process.exit(1);
});
