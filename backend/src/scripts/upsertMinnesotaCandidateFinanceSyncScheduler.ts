import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isMinnesotaCampaignFinanceEnabled, isMinnesotaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringMinnesotaCandidateFinanceSyncJobs,
  type MinnesotaCandidateFinanceSyncJobData,
} from "../scheduler/minnesotaCandidateFinanceSyncScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--raw-cache-dir", "--stale-after-days"]);

function assertNoUnknownMinnesotaFinanceArgs(args: readonly string[]): void {
  assertKnownCliFlags(args, "Minnesota candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
}

export function parseUpsertMinnesotaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): MinnesotaCandidateFinanceSyncJobData {
  assertNoUnknownMinnesotaFinanceArgs(args);

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
  const jobData = parseUpsertMinnesotaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isMinnesotaCampaignFinanceSyncEnabled();
  await upsertRecurringMinnesotaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Minnesota campaign finance recurring scheduler upserted (daily sync)"
      : "Minnesota campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Minnesota campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
