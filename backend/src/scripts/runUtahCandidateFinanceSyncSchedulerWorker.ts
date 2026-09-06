import { isUtahCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createUtahCandidateFinanceSyncSchedulerWorker } from "../scheduler/utahCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Utah campaign finance sync",
  isEnabled: isUtahCampaignFinanceEnabled,
  createWorker: createUtahCandidateFinanceSyncSchedulerWorker,
});
