import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isMichiganCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringMichiganCandidateFinanceSyncJobs,
  type MichiganCandidateFinanceSyncJobData,
} from "../scheduler/michiganCandidateFinanceSyncScheduler.js";
import { readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--stale-after-days"]);

export function parseUpsertMichiganCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): MichiganCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Michigan candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
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
  const jobData = parseUpsertMichiganCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isMichiganCampaignFinanceEnabled();
  await upsertRecurringMichiganCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Michigan campaign finance recurring scheduler upserted (daily sync)"
      : "Michigan campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Michigan campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
