import { isOklahomaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createOklahomaCandidateFinanceSyncSchedulerWorker } from "../scheduler/oklahomaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Oklahoma campaign finance sync",
  isEnabled: isOklahomaCampaignFinanceEnabled,
  createWorker: createOklahomaCandidateFinanceSyncSchedulerWorker,
});
