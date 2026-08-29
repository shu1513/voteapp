import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isMontanaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringMontanaCandidateFinanceSyncJobs,
  type MontanaCandidateFinanceSyncJobData,
} from "../scheduler/montanaCandidateFinanceSyncScheduler.js";
import {
  assertNoUnknownMontanaFinanceFlags,
  parseMontanaFinancePositiveIntegerFlag,
} from "./montanaCandidateFinanceCliArgs.js";

export function parseUpsertMontanaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): MontanaCandidateFinanceSyncJobData {
  assertNoUnknownMontanaFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseMontanaFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseMontanaFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseMontanaFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseMontanaFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertMontanaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isMontanaCampaignFinanceSyncEnabled(Boolean(jobData.force));
  await upsertRecurringMontanaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Montana campaign finance recurring scheduler upserted (daily sync)"
      : "Montana campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Montana campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
