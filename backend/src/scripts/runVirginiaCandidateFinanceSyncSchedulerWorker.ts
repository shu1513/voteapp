import { isVirginiaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createVirginiaCandidateFinanceSyncSchedulerWorker } from "../scheduler/virginiaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Virginia campaign finance sync",
  isEnabled: isVirginiaCampaignFinanceEnabled,
  createWorker: createVirginiaCandidateFinanceSyncSchedulerWorker,
});
