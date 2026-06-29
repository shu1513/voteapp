import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  isFloridaCampaignFinanceEnabled,
  isFloridaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  upsertRecurringFloridaCandidateFinanceSyncJobs,
  type FloridaCandidateFinanceSyncJobData,
} from "../scheduler/floridaCandidateFinanceSyncScheduler.js";
import { parseFloridaCandidateFinanceSyncTriggerArgs } from "./triggerFloridaCandidateFinanceSync.js";

export function parseUpsertFloridaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): FloridaCandidateFinanceSyncJobData {
  return parseFloridaCandidateFinanceSyncTriggerArgs(args);
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertFloridaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isFloridaCampaignFinanceEnabled() && isFloridaCampaignFinanceSyncEnabled();
  await upsertRecurringFloridaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Florida campaign finance recurring scheduler upserted (daily sync)"
      : "Florida campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Florida campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
