import { isGeorgiaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createGeorgiaCandidateFinanceSyncSchedulerWorker } from "../scheduler/georgiaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Georgia campaign finance sync",
  isEnabled: isGeorgiaCampaignFinanceEnabled,
  createWorker: createGeorgiaCandidateFinanceSyncSchedulerWorker,
});
