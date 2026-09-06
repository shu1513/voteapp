import { isNewYorkCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createNewYorkCandidateFinanceSyncSchedulerWorker } from "../scheduler/newYorkCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "New York campaign finance sync",
  isEnabled: isNewYorkCampaignFinanceEnabled,
  createWorker: createNewYorkCandidateFinanceSyncSchedulerWorker,
});
