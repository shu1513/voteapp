import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualMontanaCandidateFinanceSyncJob,
  type MontanaCandidateFinanceSyncJobData,
} from "../scheduler/montanaCandidateFinanceSyncScheduler.js";
import {
  assertNoUnknownMontanaFinanceFlags,
  parseMontanaFinancePositiveIntegerFlag,
} from "./montanaCandidateFinanceCliArgs.js";

export function parseMontanaCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): MontanaCandidateFinanceSyncJobData {
  assertNoUnknownMontanaFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseMontanaFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseMontanaFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseMontanaFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseMontanaFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseMontanaCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualMontanaCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Montana campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Montana campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Montana campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
