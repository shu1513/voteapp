import { isPennsylvaniaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { createPennsylvaniaCandidateFinanceSyncSchedulerWorker } from "../scheduler/pennsylvaniaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Pennsylvania campaign finance sync",
  isEnabled: () => isPennsylvaniaCampaignFinanceSyncEnabled(false),
  createWorker: createPennsylvaniaCandidateFinanceSyncSchedulerWorker,
});
