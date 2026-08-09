import { loadProjectEnv } from "../config/env.js";
import { upsertRecurringSanFranciscoFinanceJobs } from "../scheduler/sanFranciscoCandidateFinanceSyncScheduler.js";
// This upsert takes no flags: reject everything (e.g. a "--dry-run" carried
// over from the other states' CLIs) so an operator expecting a preview
// doesn't silently persist a REAL daily-write scheduler.
const unexpected = process.argv.slice(2);
if (unexpected.length > 0) {
  throw new Error(
    `San Francisco finance scheduler upsert takes no flags, got: ${unexpected.join(" ")}`,
  );
}
// Load .env before the upsert's flag check; an unloaded environment would
// read the master flag as false and remove the scheduler instead.
loadProjectEnv();
await upsertRecurringSanFranciscoFinanceJobs();
console.log(
  JSON.stringify({
    status: "ok",
    scheduler: "san_francisco_candidate_finance_sync_daily",
  }),
);
