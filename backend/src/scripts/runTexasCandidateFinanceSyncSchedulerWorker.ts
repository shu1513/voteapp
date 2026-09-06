import { isTexasCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createTexasCandidateFinanceSyncSchedulerWorker } from "../scheduler/texasCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Texas campaign finance sync",
  isEnabled: isTexasCampaignFinanceEnabled,
  createWorker: createTexasCandidateFinanceSyncSchedulerWorker,
});
