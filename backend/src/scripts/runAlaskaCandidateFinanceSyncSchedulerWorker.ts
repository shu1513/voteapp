import { isAlaskaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createAlaskaCandidateFinanceSyncSchedulerWorker } from "../scheduler/alaskaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Alaska campaign finance sync",
  isEnabled: isAlaskaCampaignFinanceEnabled,
  createWorker: createAlaskaCandidateFinanceSyncSchedulerWorker,
});
