import { isVermontCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createVermontCandidateFinanceSyncSchedulerWorker } from "../scheduler/vermontCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Vermont campaign finance sync",
  isEnabled: isVermontCampaignFinanceEnabled,
  createWorker: createVermontCandidateFinanceSyncSchedulerWorker,
});
