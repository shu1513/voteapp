import { createLosAngelesCityFinanceWorker } from "../scheduler/losAngelesCityCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Los Angeles City candidate finance",
  isEnabled: () => true,
  createWorker: createLosAngelesCityFinanceWorker,
});
