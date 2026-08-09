import { upsertRecurringLosAngelesCityFinanceJobs } from "../scheduler/losAngelesCityCandidateFinanceSyncScheduler.js";
// This upsert takes no flags: reject everything (e.g. a "--dry-run" carried
// over from the other states' CLIs) so an operator expecting a preview
// doesn't silently persist a REAL daily-write scheduler.
const unexpected = process.argv.slice(2);
if (unexpected.length > 0) {
  throw new Error(
    `Los Angeles City finance scheduler upsert takes no flags, got: ${unexpected.join(" ")}`,
  );
}
await upsertRecurringLosAngelesCityFinanceJobs();
console.log(
  JSON.stringify({
    status: "ok",
    scheduler: "los_angeles_city_candidate_finance_sync_daily",
  }),
);
