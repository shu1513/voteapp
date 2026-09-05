import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualDistrictOfColumbiaCandidateFinanceSyncJob,
  type DistrictOfColumbiaCandidateFinanceSyncJobData,
} from "../scheduler/districtOfColumbiaCandidateFinanceSyncScheduler.js";
import { readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--stale-after-days"]);

export function parseDistrictOfColumbiaCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): DistrictOfColumbiaCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "District of Columbia candidate finance sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
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
  const jobData = parseDistrictOfColumbiaCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualDistrictOfColumbiaCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("D.C. campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `D.C. campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("D.C. campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
