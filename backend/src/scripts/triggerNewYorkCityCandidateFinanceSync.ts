import { pathToFileURL } from "node:url";
import { loadProjectEnv } from "../config/env.js";
import { enqueueNewYorkCityFinanceSyncJob } from "../scheduler/newYorkCityCandidateFinanceSyncScheduler.js";

async function main(): Promise<void> {
  loadProjectEnv();
  const args = process.argv.slice(2);
  const id = await enqueueNewYorkCityFinanceSyncJob({
    force: args.includes("--force"),
    dryRun: args.includes("--dry-run"),
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
  });
  console.log(JSON.stringify({ type: "new_york_city_finance_sync_enqueued", job_id: id }));
}
if ((process.argv[1] ? pathToFileURL(process.argv[1]).href : null) === import.meta.url) {
  main().catch((error) => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
