import { isColoradoCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createColoradoCandidateFinanceSyncSchedulerWorker } from "../scheduler/coloradoCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Colorado campaign finance sync",
  isEnabled: isColoradoCampaignFinanceEnabled,
  createWorker: createColoradoCandidateFinanceSyncSchedulerWorker,
});
