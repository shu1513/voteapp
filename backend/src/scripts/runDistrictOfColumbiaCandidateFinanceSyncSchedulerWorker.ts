import { isDistrictOfColumbiaCampaignFinanceEnabled } from "../config/featureFlags.js";
import { createDistrictOfColumbiaCandidateFinanceSyncSchedulerWorker } from "../scheduler/districtOfColumbiaCandidateFinanceSyncScheduler.js";
import { runFinanceSchedulerWorker } from "../scheduler/financeSchedulerWorkerRunner.js";

runFinanceSchedulerWorker({
  label: "D.C. campaign finance sync",
  isEnabled: isDistrictOfColumbiaCampaignFinanceEnabled,
  createWorker: createDistrictOfColumbiaCandidateFinanceSyncSchedulerWorker,
});
