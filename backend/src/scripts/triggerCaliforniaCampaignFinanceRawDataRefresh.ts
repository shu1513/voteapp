import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualCaliforniaCampaignFinanceRawDataRefreshJob,
  type CaliforniaCampaignFinanceRawDataRefreshJobData,
} from "../scheduler/californiaCampaignFinanceRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url"]);

export function parseCaliforniaCampaignFinanceRawDataRefreshTriggerArgs(
  args: readonly string[]
): CaliforniaCampaignFinanceRawDataRefreshJobData {
  assertKnownCliFlags(args, "California campaign finance raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    url: readStrictFlagValue(args, "--url")?.trim() || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir")?.trim() || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseCaliforniaCampaignFinanceRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualCaliforniaCampaignFinanceRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("California campaign finance raw-data refresh is disabled; job was not enqueued");
    return;
  }
  console.log(
    `California campaign finance raw-data refresh job enqueued (jobId=${jobId} force=${Boolean(jobData.force)})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("California campaign finance raw-data refresh trigger failed:", error);
    process.exit(1);
  });
}
