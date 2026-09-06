import { isConnecticutCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createConnecticutCandidateFinanceSyncSchedulerWorker } from "../scheduler/connecticutCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Connecticut campaign finance sync",
  isEnabled: isConnecticutCampaignFinanceEnabled,
  createWorker: createConnecticutCandidateFinanceSyncSchedulerWorker,
});
