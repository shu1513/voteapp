import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualSouthCarolinaCandidateFinanceSyncJob,
  type SouthCarolinaCandidateFinanceSyncJobData,
} from "../scheduler/southCarolinaCandidateFinanceSyncScheduler.js";
import {
  assertNoUnknownSouthCarolinaFinanceFlags,
  parseSouthCarolinaFinancePositiveIntegerFlag,
} from "./southCarolinaCandidateFinanceCliArgs.js";

export function parseSouthCarolinaCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): SouthCarolinaCandidateFinanceSyncJobData {
  assertNoUnknownSouthCarolinaFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseSouthCarolinaCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualSouthCarolinaCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("South Carolina campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `South Carolina campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("South Carolina campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
