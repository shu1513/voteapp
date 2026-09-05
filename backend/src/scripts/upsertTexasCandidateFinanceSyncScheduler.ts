import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isTexasCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringTexasCandidateFinanceSyncJobs,
  type TexasCandidateFinanceSyncJobData,
} from "../scheduler/texasCandidateFinanceSyncScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--raw-cache-dir", "--raw-zip", "--stale-after-days"]);

export function parseUpsertTexasCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): TexasCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Texas candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
    rawDataZipPath: readStrictFlagValue(args, "--raw-zip") || undefined,
    rawDataCacheDir: readStrictFlagValue(args, "--raw-cache-dir") || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertTexasCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isTexasCampaignFinanceEnabled();
  await upsertRecurringTexasCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Texas campaign finance recurring scheduler upserted (daily sync)"
      : "Texas campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Texas campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
