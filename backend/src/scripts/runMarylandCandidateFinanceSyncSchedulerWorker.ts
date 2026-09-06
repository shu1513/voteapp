import { isMarylandCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createMarylandCandidateFinanceSyncSchedulerWorker } from "../scheduler/marylandCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Maryland campaign finance sync",
  isEnabled: isMarylandCampaignFinanceEnabled,
  createWorker: createMarylandCandidateFinanceSyncSchedulerWorker,
});
