import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isSouthCarolinaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringSouthCarolinaCandidateFinanceSyncJobs,
  type SouthCarolinaCandidateFinanceSyncJobData,
} from "../scheduler/southCarolinaCandidateFinanceSyncScheduler.js";
import {
  assertNoUnknownSouthCarolinaFinanceFlags,
  parseSouthCarolinaFinancePositiveIntegerFlag,
} from "./southCarolinaCandidateFinanceCliArgs.js";

export function parseUpsertSouthCarolinaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): SouthCarolinaCandidateFinanceSyncJobData {
  assertNoUnknownSouthCarolinaFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertSouthCarolinaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isSouthCarolinaCampaignFinanceSyncEnabled(Boolean(jobData.force));
  await upsertRecurringSouthCarolinaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "South Carolina campaign finance recurring scheduler upserted (daily sync)"
      : "South Carolina campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("South Carolina campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
