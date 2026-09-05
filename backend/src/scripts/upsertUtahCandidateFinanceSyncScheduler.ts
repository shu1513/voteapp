import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isUtahCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringUtahCandidateFinanceSyncJobs,
  type UtahCandidateFinanceSyncJobData,
} from "../scheduler/utahCandidateFinanceSyncScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force", "--refresh-cache"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--raw-cache-dir", "--stale-after-days"]);

export function parseUpsertUtahCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): UtahCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Utah candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
    rawDataCacheDir: readStrictFlagValue(args, "--raw-cache-dir") || undefined,
    refreshCache: args.includes("--refresh-cache"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertUtahCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isUtahCampaignFinanceEnabled();
  await upsertRecurringUtahCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Utah campaign finance recurring scheduler upserted (daily sync)"
      : "Utah campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Utah campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
