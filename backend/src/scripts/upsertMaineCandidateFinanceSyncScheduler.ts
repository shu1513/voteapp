import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isMaineCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringMaineCandidateFinanceSyncJobs,
  type MaineCandidateFinanceSyncJobData,
} from "../scheduler/maineCandidateFinanceSyncScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--raw-cache-dir",
]);

function validateKnownFlags(args: readonly string[]): void {
  assertKnownCliFlags(args, "Maine candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
}

export function parseUpsertMaineCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): MaineCandidateFinanceSyncJobData {
  validateKnownFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
    rawDataCacheDir: readStrictFlagValue(args, "--raw-cache-dir") || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertMaineCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isMaineCampaignFinanceEnabled();
  await upsertRecurringMaineCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Maine campaign finance recurring scheduler upserted (daily sync)"
      : "Maine campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Maine campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
