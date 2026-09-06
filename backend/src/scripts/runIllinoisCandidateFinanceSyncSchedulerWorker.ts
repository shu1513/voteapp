import { isIllinoisCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { createIllinoisCandidateFinanceSyncSchedulerWorker } from "../scheduler/illinoisCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Illinois campaign finance sync",
  isEnabled: isIllinoisCampaignFinanceSyncEnabled,
  createWorker: createIllinoisCandidateFinanceSyncSchedulerWorker,
});
