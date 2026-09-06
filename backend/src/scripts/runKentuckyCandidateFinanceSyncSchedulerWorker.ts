import { isKentuckyCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createKentuckyCandidateFinanceSyncSchedulerWorker } from "../scheduler/kentuckyCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Kentucky campaign finance sync",
  isEnabled: isKentuckyCampaignFinanceEnabled,
  createWorker: createKentuckyCandidateFinanceSyncSchedulerWorker,
});
