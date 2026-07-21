import { pathToFileURL } from "node:url";
import { loadProjectEnv } from "../config/env.js";
import { upsertRecurringNewYorkCityFinanceSyncJob } from "../scheduler/newYorkCityCandidateFinanceSyncScheduler.js";

async function main(): Promise<void> {
  loadProjectEnv();
  await upsertRecurringNewYorkCityFinanceSyncJob();
  console.log(JSON.stringify({ type: "new_york_city_finance_scheduler_upserted" }));
}
if ((process.argv[1] ? pathToFileURL(process.argv[1]).href : null) === import.meta.url) {
  main().catch((error) => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
