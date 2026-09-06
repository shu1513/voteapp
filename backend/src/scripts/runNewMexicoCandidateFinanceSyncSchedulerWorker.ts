import { isNewMexicoCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createNewMexicoCandidateFinanceSyncSchedulerWorker } from "../scheduler/newMexicoCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "New Mexico campaign finance sync",
  isEnabled: isNewMexicoCampaignFinanceEnabled,
  createWorker: createNewMexicoCandidateFinanceSyncSchedulerWorker,
});
