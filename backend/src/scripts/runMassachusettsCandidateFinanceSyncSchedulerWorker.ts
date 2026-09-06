import { isMassachusettsCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createMassachusettsCandidateFinanceSyncSchedulerWorker } from "../scheduler/massachusettsCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Massachusetts campaign finance sync",
  isEnabled: isMassachusettsCampaignFinanceEnabled,
  createWorker: createMassachusettsCandidateFinanceSyncSchedulerWorker,
});
