import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualOregonCandidateFinanceSyncJob,
  type OregonCandidateFinanceSyncJobData,
} from "../scheduler/oregonCandidateFinanceSyncScheduler.js";
import { readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--direct-max-breakdowns", "--lookahead-days", "--lookback-days", "--max-candidates", "--min-industry-amount", "--outside-max-breakdowns", "--outside-max-groups", "--stale-after-days"]);

export function parseOregonCandidateFinanceSyncTriggerArgs(args: readonly string[]): OregonCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Oregon candidate finance sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
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
  const jobData = parseOregonCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualOregonCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Oregon campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Oregon campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Oregon campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
