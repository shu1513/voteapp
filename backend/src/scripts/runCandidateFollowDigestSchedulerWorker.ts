import { createCandidateFollowDigestSchedulerWorker } from "../scheduler/candidateFollowDigestScheduler.js";
import { runSchedulerWorker } from "../scheduler/schedulerWorkerRunner.js";

runSchedulerWorker("candidate_follow_digest", createCandidateFollowDigestSchedulerWorker);
