import { isMinnesotaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createMinnesotaCandidateFinanceSyncSchedulerWorker } from "../scheduler/minnesotaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Minnesota campaign finance sync",
  isEnabled: isMinnesotaCampaignFinanceEnabled,
  createWorker: createMinnesotaCandidateFinanceSyncSchedulerWorker,
});
