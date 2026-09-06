import { isCaliforniaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createCaliforniaCandidateFinanceSyncSchedulerWorker } from "../scheduler/californiaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "California campaign finance sync",
  isEnabled: isCaliforniaCampaignFinanceEnabled,
  createWorker: createCaliforniaCandidateFinanceSyncSchedulerWorker,
});
