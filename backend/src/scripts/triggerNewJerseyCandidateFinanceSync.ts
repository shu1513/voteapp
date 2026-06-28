import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualNewJerseyCandidateFinanceSyncJob,
  type NewJerseyCandidateFinanceSyncJobData,
} from "../scheduler/newJerseyCandidateFinanceSyncScheduler.js";
import {
  assertKnownNewJerseyCampaignFinanceFlags,
  parseNewJerseyCampaignFinanceBooleanFlag,
  parseNewJerseyCampaignFinancePositiveIntegerFlag,
} from "./newJerseyCandidateFinanceCliArgs.js";

const TRIGGER_NEW_JERSEY_CANDIDATE_FINANCE_SYNC_FLAGS = new Set([
  "--dry-run",
  "--force",
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
]);
const TRIGGER_NEW_JERSEY_CANDIDATE_FINANCE_SYNC_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
]);

export function parseNewJerseyCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): NewJerseyCandidateFinanceSyncJobData {
  assertKnownNewJerseyCampaignFinanceFlags(
    args,
    TRIGGER_NEW_JERSEY_CANDIDATE_FINANCE_SYNC_FLAGS,
    TRIGGER_NEW_JERSEY_CANDIDATE_FINANCE_SYNC_VALUE_FLAGS
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
  const jobData = parseNewJerseyCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualNewJerseyCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("New Jersey campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `New Jersey campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("New Jersey campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
