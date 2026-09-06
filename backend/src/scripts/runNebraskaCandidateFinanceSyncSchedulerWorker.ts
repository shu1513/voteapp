import { isNebraskaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createNebraskaCandidateFinanceSyncSchedulerWorker } from "../scheduler/nebraskaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Nebraska campaign finance sync",
  isEnabled: isNebraskaCampaignFinanceEnabled,
  createWorker: createNebraskaCandidateFinanceSyncSchedulerWorker,
});
