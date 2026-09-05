import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualTexasTecRawDataRefreshJob,
  type TexasTecRawDataRefreshJobData,
} from "../scheduler/texasTecRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url"]);

export function parseTexasTecRawDataRefreshTriggerArgs(args: readonly string[]): TexasTecRawDataRefreshJobData {
  assertKnownCliFlags(args, "Texas TEC raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    url: readStrictFlagValue(args, "--url") || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseTexasTecRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualTexasTecRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("Texas TEC raw-data refresh is disabled; job was not enqueued");
    return;
  }
  console.log(`Texas TEC raw-data refresh job enqueued (jobId=${jobId} force=${Boolean(jobData.force)})`);
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Texas TEC raw-data refresh trigger failed:", error);
    process.exit(1);
  });
}
