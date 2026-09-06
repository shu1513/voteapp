import { createSanFranciscoFinanceWorker } from "../scheduler/sanFranciscoCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "San Francisco candidate finance",
  isEnabled: () => true,
  createWorker: createSanFranciscoFinanceWorker,
});
