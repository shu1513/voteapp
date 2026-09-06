import { isHoustonCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createHoustonCandidateFinanceSyncSchedulerWorker } from "../scheduler/houstonCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Houston campaign finance sync",
  isEnabled: isHoustonCampaignFinanceEnabled,
  createWorker: createHoustonCandidateFinanceSyncSchedulerWorker,
});
