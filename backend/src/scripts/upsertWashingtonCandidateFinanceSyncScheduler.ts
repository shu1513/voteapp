import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isWashingtonCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringWashingtonCandidateFinanceSyncJobs,
  type WashingtonCandidateFinanceSyncJobData,
} from "../scheduler/washingtonCandidateFinanceSyncScheduler.js";
import { readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--stale-after-days"]);

export function parseUpsertWashingtonCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): WashingtonCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Washington candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertWashingtonCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isWashingtonCampaignFinanceEnabled();
  await upsertRecurringWashingtonCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Washington campaign finance recurring scheduler upserted (daily sync)"
      : "Washington campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Washington campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
