import { isOhioCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createOhioCandidateFinanceSyncSchedulerWorker } from "../scheduler/ohioCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Ohio campaign finance sync",
  isEnabled: isOhioCampaignFinanceEnabled,
  createWorker: createOhioCandidateFinanceSyncSchedulerWorker,
});
