import { createNewElectionAlertSchedulerWorker } from "../scheduler/newElectionAlertScheduler.js";
import { runSchedulerWorker } from "../scheduler/schedulerWorkerRunner.js";

runSchedulerWorker("new_election_alert", createNewElectionAlertSchedulerWorker);
