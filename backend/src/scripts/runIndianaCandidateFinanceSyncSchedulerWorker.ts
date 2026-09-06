import { isIndianaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createIndianaCandidateFinanceSyncSchedulerWorker } from "../scheduler/indianaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Indiana campaign finance sync",
  isEnabled: isIndianaCampaignFinanceEnabled,
  createWorker: createIndianaCandidateFinanceSyncSchedulerWorker,
});
