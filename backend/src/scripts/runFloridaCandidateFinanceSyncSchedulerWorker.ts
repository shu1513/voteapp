import { isFloridaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createFloridaCandidateFinanceSyncSchedulerWorker } from "../scheduler/floridaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Florida campaign finance sync",
  isEnabled: isFloridaCampaignFinanceEnabled,
  createWorker: createFloridaCandidateFinanceSyncSchedulerWorker,
});
