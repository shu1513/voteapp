import { isNorthCarolinaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createNorthCarolinaCandidateFinanceSyncSchedulerWorker } from "../scheduler/northCarolinaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "North Carolina campaign finance sync",
  isEnabled: isNorthCarolinaCampaignFinanceEnabled,
  createWorker: createNorthCarolinaCandidateFinanceSyncSchedulerWorker,
});
