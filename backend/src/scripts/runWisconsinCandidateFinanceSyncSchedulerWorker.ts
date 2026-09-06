import { isWisconsinCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createWisconsinCandidateFinanceSyncSchedulerWorker } from "../scheduler/wisconsinCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Wisconsin campaign finance sync",
  isEnabled: isWisconsinCampaignFinanceEnabled,
  createWorker: createWisconsinCandidateFinanceSyncSchedulerWorker,
});
