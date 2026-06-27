import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isNewJerseyCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringNewJerseyCandidateFinanceSyncJobs,
  type NewJerseyCandidateFinanceSyncJobData,
} from "../scheduler/newJerseyCandidateFinanceSyncScheduler.js";
import {
  assertKnownNewJerseyCampaignFinanceFlags,
  parseNewJerseyCampaignFinanceBooleanFlag,
  parseNewJerseyCampaignFinancePositiveIntegerFlag,
} from "./newJerseyCandidateFinanceCliArgs.js";

const UPSERT_NEW_JERSEY_CANDIDATE_FINANCE_SYNC_SCHEDULER_FLAGS = new Set([
  "--dry-run",
  "--force",
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
]);
const UPSERT_NEW_JERSEY_CANDIDATE_FINANCE_SYNC_SCHEDULER_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
]);

export function parseUpsertNewJerseyCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): NewJerseyCandidateFinanceSyncJobData {
  assertKnownNewJerseyCampaignFinanceFlags(
    args,
    UPSERT_NEW_JERSEY_CANDIDATE_FINANCE_SYNC_SCHEDULER_FLAGS,
    UPSERT_NEW_JERSEY_CANDIDATE_FINANCE_SYNC_SCHEDULER_VALUE_FLAGS
  );
  return {
    dryRun: parseNewJerseyCampaignFinanceBooleanFlag(args, "--dry-run"),
    force: parseNewJerseyCampaignFinanceBooleanFlag(args, "--force"),
    maxCandidates: parseNewJerseyCampaignFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseNewJerseyCampaignFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseNewJerseyCampaignFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseNewJerseyCampaignFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertNewJerseyCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isNewJerseyCampaignFinanceEnabled();
  await upsertRecurringNewJerseyCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "New Jersey campaign finance recurring scheduler upserted (daily sync)"
      : "New Jersey campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("New Jersey campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
