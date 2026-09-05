import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isIndianaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringIndianaCandidateFinanceSyncJobs,
  type IndianaCandidateFinanceSyncJobData,
} from "../scheduler/indianaCandidateFinanceSyncScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownIndianaCampaignFinanceCliArgs } from "./indianaCampaignFinanceCliArgs.js";

export function parseUpsertIndianaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): IndianaCandidateFinanceSyncJobData {
  assertKnownIndianaCampaignFinanceCliArgs(args, [
    { name: "--dry-run", takesValue: false },
    { name: "--force", takesValue: false },
    { name: "--max-candidates", takesValue: true },
    { name: "--stale-after-days", takesValue: true },
    { name: "--lookback-days", takesValue: true },
    { name: "--lookahead-days", takesValue: true },
    { name: "--raw-cache-dir", takesValue: true },
  ]);
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
  const jobData = parseUpsertIndianaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isIndianaCampaignFinanceEnabled();
  await upsertRecurringIndianaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Indiana campaign finance recurring scheduler upserted (daily sync)"
      : "Indiana campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Indiana campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
