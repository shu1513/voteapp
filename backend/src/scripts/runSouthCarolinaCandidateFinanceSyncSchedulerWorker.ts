import { isSouthCarolinaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createSouthCarolinaCandidateFinanceSyncSchedulerWorker } from "../scheduler/southCarolinaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "South Carolina campaign finance sync",
  isEnabled: isSouthCarolinaCampaignFinanceEnabled,
  createWorker: createSouthCarolinaCandidateFinanceSyncSchedulerWorker,
});
