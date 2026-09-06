import { isCandidateFinanceEnabled } from "../config/featureFlags.js";
import { createCandidateFinanceSyncSchedulerWorker } from "../scheduler/candidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "candidate_finance sync",
  isEnabled: isCandidateFinanceEnabled,
  createWorker: createCandidateFinanceSyncSchedulerWorker,
});
