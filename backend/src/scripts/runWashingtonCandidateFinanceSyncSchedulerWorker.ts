import { isWashingtonCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createWashingtonCandidateFinanceSyncSchedulerWorker } from "../scheduler/washingtonCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Washington campaign finance sync",
  isEnabled: isWashingtonCampaignFinanceEnabled,
  createWorker: createWashingtonCandidateFinanceSyncSchedulerWorker,
});
