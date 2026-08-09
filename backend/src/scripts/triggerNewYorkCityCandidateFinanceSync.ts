import { pathToFileURL } from "node:url";
import { loadProjectEnv } from "../config/env.js";
import {
  enqueueNewYorkCityFinanceSyncJob,
  type NewYorkCityFinanceSyncJobData,
} from "../scheduler/newYorkCityCandidateFinanceSyncScheduler.js";

const KNOWN_FLAGS = new Set(["--dry-run", "--force"]);

// Strict like the Ohio/Georgia trigger CLIs: a typo (--dryrun), an inline
// value (--dry-run=true), or a bare positional ("dry-run" after npm's own
// "--" separator) must fail loudly instead of silently enqueueing a REAL
// sync. This trigger has no value flags, so every argument must match a
// known boolean flag exactly.
export function parseNewYorkCityCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): NewYorkCityFinanceSyncJobData {
  for (const arg of args) {
    if (!KNOWN_FLAGS.has(arg)) {
      throw new Error(`Unknown New York City candidate finance sync flag: ${arg}`);
    }
  }
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseNewYorkCityCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const id = await enqueueNewYorkCityFinanceSyncJob(jobData);
  console.log(JSON.stringify({ type: "new_york_city_finance_sync_enqueued", job_id: id }));
}
if ((process.argv[1] ? pathToFileURL(process.argv[1]).href : null) === import.meta.url) {
  main().catch((error) => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
