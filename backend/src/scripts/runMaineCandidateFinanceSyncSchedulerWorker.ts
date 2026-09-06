import { isMaineCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createMaineCandidateFinanceSyncSchedulerWorker } from "../scheduler/maineCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Maine campaign finance sync",
  isEnabled: isMaineCampaignFinanceEnabled,
  createWorker: createMaineCandidateFinanceSyncSchedulerWorker,
});
