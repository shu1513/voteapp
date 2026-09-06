import { isArizonaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createArizonaCandidateFinanceSyncSchedulerWorker } from "../scheduler/arizonaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Arizona campaign finance sync",
  isEnabled: isArizonaCampaignFinanceEnabled,
  createWorker: createArizonaCandidateFinanceSyncSchedulerWorker,
});
