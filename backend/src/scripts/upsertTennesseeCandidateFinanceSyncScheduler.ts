import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isTennesseeCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringTennesseeCandidateFinanceSyncJobs,
  type TennesseeCandidateFinanceSyncJobData,
} from "../scheduler/tennesseeCandidateFinanceSyncScheduler.js";
import {
  assertNoUnknownTennesseeFinanceFlags,
  parseTennesseeFinancePositiveIntegerFlag,
} from "./tennesseeCandidateFinanceCliArgs.js";

export function parseUpsertTennesseeCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): TennesseeCandidateFinanceSyncJobData {
  assertNoUnknownTennesseeFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseTennesseeFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseTennesseeFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseTennesseeFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseTennesseeFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertTennesseeCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isTennesseeCampaignFinanceSyncEnabled(Boolean(jobData.force));
  await upsertRecurringTennesseeCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Tennessee campaign finance recurring scheduler upserted (daily sync)"
      : "Tennessee campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Tennessee campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
