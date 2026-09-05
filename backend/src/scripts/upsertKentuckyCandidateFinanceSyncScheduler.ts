import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isKentuckyCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringKentuckyCandidateFinanceSyncJobs,
  type KentuckyCandidateFinanceSyncJobData,
} from "../scheduler/kentuckyCandidateFinanceSyncScheduler.js";
import { readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

function parseAutoLinkMissingLinksFlag(args: readonly string[]): boolean {
  const enabled = args.includes("--auto-link");
  const disabled = args.includes("--no-auto-link");
  if (enabled && disabled) {
    throw new Error("Provide either --auto-link or --no-auto-link, not both");
  }
  return !disabled;
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--auto-link", "--dry-run", "--force", "--no-auto-link"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--stale-after-days"]);

export function parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): KentuckyCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Kentucky candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
    autoLinkMissingLinks: parseAutoLinkMissingLinksFlag(args),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isKentuckyCampaignFinanceEnabled();
  await upsertRecurringKentuckyCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Kentucky campaign finance recurring scheduler upserted (daily sync)"
      : "Kentucky campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Kentucky campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
