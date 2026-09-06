import { isHawaiiCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createHawaiiCandidateFinanceSyncSchedulerWorker } from "../scheduler/hawaiiCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Hawaii campaign finance sync",
  isEnabled: isHawaiiCampaignFinanceEnabled,
  createWorker: createHawaiiCandidateFinanceSyncSchedulerWorker,
});
