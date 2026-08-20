import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { enqueueManualMissouriCandidateFinanceSyncJob } from "../scheduler/missouriCandidateFinanceSyncScheduler.js";
import { parseMissouriCandidateFinanceCliArgs } from "./missouriCandidateFinanceCli.js";

async function main(): Promise<void> {
  loadProjectEnv();
  const jobId = await enqueueManualMissouriCandidateFinanceSyncJob(parseMissouriCandidateFinanceCliArgs(process.argv.slice(2)));
  console.log(`Missouri campaign finance sync job ${jobId}`);
}

if ((process.argv[1] ? pathToFileURL(process.argv[1]).href : null) === import.meta.url) {
  main().catch((error) => { console.error("Missouri finance sync enqueue failed:", error); process.exit(1); });
}
