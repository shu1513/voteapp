import { createNewYorkCityFinanceSyncWorker } from "../scheduler/newYorkCityCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "NYC candidate finance",
  isEnabled: () => true,
  createWorker: createNewYorkCityFinanceSyncWorker,
});
