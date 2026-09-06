import { isTennesseeCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createTennesseeCandidateFinanceSyncSchedulerWorker } from "../scheduler/tennesseeCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Tennessee campaign finance sync",
  isEnabled: isTennesseeCampaignFinanceEnabled,
  createWorker: createTennesseeCandidateFinanceSyncSchedulerWorker,
});
