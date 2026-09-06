import { isMissouriCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createMissouriCandidateFinanceSyncSchedulerWorker } from "../scheduler/missouriCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "Missouri campaign finance sync",
  isEnabled: isMissouriCampaignFinanceEnabled,
  createWorker: createMissouriCandidateFinanceSyncSchedulerWorker,
});
