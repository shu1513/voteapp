import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualNebraskaCandidateFinanceSyncJob,
  type NebraskaCandidateFinanceSyncJobData,
} from "../scheduler/nebraskaCandidateFinanceSyncScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--raw-cache-dir",
  "--raw-zip",
]);

function assertNoUnknownNebraskaFinanceTriggerArgs(args: readonly string[]): void {
  assertKnownCliFlags(args, "Nebraska candidate finance sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
}

export function parseNebraskaCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): NebraskaCandidateFinanceSyncJobData {
  assertNoUnknownNebraskaFinanceTriggerArgs(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
    rawDataCacheDir: readStrictFlagValue(args, "--raw-cache-dir") || undefined,
    rawDataZipPath: readStrictFlagValue(args, "--raw-zip") || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseNebraskaCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualNebraskaCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Nebraska campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Nebraska campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Nebraska campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
