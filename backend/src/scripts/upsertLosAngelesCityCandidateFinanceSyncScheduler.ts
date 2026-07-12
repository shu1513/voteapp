import { upsertRecurringLosAngelesCityFinanceJobs } from "../scheduler/losAngelesCityCandidateFinanceSyncScheduler.js";
await upsertRecurringLosAngelesCityFinanceJobs();
console.log(
  JSON.stringify({
    status: "ok",
    scheduler: "los_angeles_city_candidate_finance_sync_daily",
  }),
);
