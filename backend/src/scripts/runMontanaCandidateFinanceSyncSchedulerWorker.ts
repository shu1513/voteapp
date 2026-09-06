import { isMontanaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createMontanaCandidateFinanceSyncSchedulerWorker } from "../scheduler/montanaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Montana campaign finance sync",
  isEnabled: isMontanaCampaignFinanceEnabled,
  createWorker: createMontanaCandidateFinanceSyncSchedulerWorker,
});
