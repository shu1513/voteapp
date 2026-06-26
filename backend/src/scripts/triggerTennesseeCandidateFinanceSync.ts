import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualTennesseeCandidateFinanceSyncJob,
  type TennesseeCandidateFinanceSyncJobData,
} from "../scheduler/tennesseeCandidateFinanceSyncScheduler.js";
import {
  assertNoUnknownTennesseeFinanceFlags,
  parseTennesseeFinancePositiveIntegerFlag,
} from "./tennesseeCandidateFinanceCliArgs.js";

export function parseTennesseeCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): TennesseeCandidateFinanceSyncJobData {
  assertNoUnknownTennesseeFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseTennesseeFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseTennesseeFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseTennesseeFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseTennesseeFinancePositiveIntegerFlag(args, "--lookahead-days"),
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
    aiClassificationMinAmount: parseTennesseeFinancePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseTennesseeCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualTennesseeCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Tennessee campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Tennessee campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Tennessee campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
