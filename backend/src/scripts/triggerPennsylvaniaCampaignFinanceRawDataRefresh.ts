import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualPennsylvaniaCampaignFinanceRawDataRefreshJob,
  type PennsylvaniaCampaignFinanceRawDataRefreshJobData,
} from "../scheduler/pennsylvaniaCampaignFinanceRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url", "--year"]);

export function parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs(
  args: readonly string[]
): PennsylvaniaCampaignFinanceRawDataRefreshJobData {
  assertKnownCliFlags(args, "Pennsylvania campaign finance raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    year: readStrictPositiveIntegerFlag(args, "--year"),
    force: args.includes("--force"),
    url: readStrictFlagValue(args, "--url")?.trim() || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir")?.trim() || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualPennsylvaniaCampaignFinanceRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("Pennsylvania campaign finance raw-data refresh is disabled; job was not enqueued");
    return;
  }
  // Unpinned jobs resolve both cycle years at run time (previous + current).
  const yearScope = jobData.year !== undefined ? `year=${jobData.year}` : "years=cycle(previous+current)";
  console.log(
    `Pennsylvania campaign finance raw-data refresh job enqueued (jobId=${jobId} ${yearScope} force=${Boolean(
      jobData.force
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Pennsylvania campaign finance raw-data refresh trigger failed:", error);
    process.exit(1);
  });
}
