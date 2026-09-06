import { isNewJerseyCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createNewJerseyCandidateFinanceSyncSchedulerWorker } from "../scheduler/newJerseyCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "New Jersey campaign finance sync",
  isEnabled: isNewJerseyCampaignFinanceEnabled,
  createWorker: createNewJerseyCandidateFinanceSyncSchedulerWorker,
});
