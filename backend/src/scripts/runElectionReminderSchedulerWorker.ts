import { createElectionReminderSchedulerWorker } from "../scheduler/electionReminderScheduler.js";
import { runSchedulerWorker } from "../scheduler/schedulerWorkerRunner.js";

runSchedulerWorker("election_reminder", createElectionReminderSchedulerWorker);
