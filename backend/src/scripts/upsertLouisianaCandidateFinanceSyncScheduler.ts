import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isLouisianaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringLouisianaCandidateFinanceSyncJobs,
  type LouisianaCandidateFinanceSyncJobData,
} from "../scheduler/louisianaCandidateFinanceSyncScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--raw-cache-dir", "--stale-after-days"]);

function assertNoUnknownLouisianaFinanceArgs(args: readonly string[]): void {
  assertKnownCliFlags(args, "Louisiana candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
}

export function parseUpsertLouisianaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): LouisianaCandidateFinanceSyncJobData {
  assertNoUnknownLouisianaFinanceArgs(args);

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
  const jobData = parseUpsertLouisianaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isLouisianaCampaignFinanceSyncEnabled();
  await upsertRecurringLouisianaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Louisiana campaign finance recurring scheduler upserted (daily sync)"
      : "Louisiana campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Louisiana campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
