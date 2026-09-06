import { isLouisianaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createLouisianaCandidateFinanceSyncSchedulerWorker } from "../scheduler/louisianaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Louisiana campaign finance sync",
  isEnabled: isLouisianaCampaignFinanceEnabled,
  createWorker: createLouisianaCandidateFinanceSyncSchedulerWorker,
});
