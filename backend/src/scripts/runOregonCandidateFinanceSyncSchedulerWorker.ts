import { isOregonCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createOregonCandidateFinanceSyncSchedulerWorker } from "../scheduler/oregonCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Oregon campaign finance sync",
  isEnabled: isOregonCampaignFinanceEnabled,
  createWorker: createOregonCandidateFinanceSyncSchedulerWorker,
});
