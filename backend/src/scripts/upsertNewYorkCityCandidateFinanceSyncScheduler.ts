import { pathToFileURL } from "node:url";
import { loadProjectEnv } from "../config/env.js";
import { upsertRecurringNewYorkCityFinanceSyncJob } from "../scheduler/newYorkCityCandidateFinanceSyncScheduler.js";

async function main(): Promise<void> {
  // This upsert takes no flags: reject everything (e.g. a "--dry-run"
  // carried over from the other states' CLIs) so an operator expecting a
  // preview doesn't silently persist a REAL daily-write scheduler.
  const unexpected = process.argv.slice(2);
  if (unexpected.length > 0) {
    throw new Error(
      `New York City finance scheduler upsert takes no flags, got: ${unexpected.join(" ")}`
    );
  }
  loadProjectEnv();
  await upsertRecurringNewYorkCityFinanceSyncJob();
  console.log(JSON.stringify({ type: "new_york_city_finance_scheduler_upserted" }));
}
if ((process.argv[1] ? pathToFileURL(process.argv[1]).href : null) === import.meta.url) {
  main().catch((error) => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
