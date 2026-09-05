import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isOregonCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringOregonCandidateFinanceSyncJobs,
  type OregonCandidateFinanceSyncJobData,
} from "../scheduler/oregonCandidateFinanceSyncScheduler.js";
import { readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--direct-max-breakdowns", "--lookahead-days", "--lookback-days", "--max-candidates", "--min-industry-amount", "--outside-max-breakdowns", "--outside-max-groups", "--stale-after-days"]);

export function parseUpsertOregonCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): OregonCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Oregon candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
    directMaxBreakdownsPerCategory: readStrictPositiveIntegerFlag(args, "--direct-max-breakdowns"),
    outsideMaxGroups: readStrictPositiveIntegerFlag(args, "--outside-max-groups"),
    outsideMaxBreakdownsPerCategory: readStrictPositiveIntegerFlag(args, "--outside-max-breakdowns"),
    minIndustryAmount: readStrictPositiveIntegerFlag(args, "--min-industry-amount"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertOregonCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isOregonCampaignFinanceEnabled();
  await upsertRecurringOregonCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Oregon campaign finance recurring scheduler upserted (daily sync)"
      : "Oregon campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Oregon campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
