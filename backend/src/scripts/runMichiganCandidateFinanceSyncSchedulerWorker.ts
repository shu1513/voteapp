import { isMichiganCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { createMichiganCandidateFinanceSyncSchedulerWorker } from "../scheduler/michiganCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Michigan campaign finance sync",
  isEnabled: () => isMichiganCampaignFinanceSyncEnabled(false),
  createWorker: createMichiganCandidateFinanceSyncSchedulerWorker,
});
