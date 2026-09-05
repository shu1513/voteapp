import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualColoradoTracerRawDataRefreshJob,
  type ColoradoTracerRawDataRefreshJobData,
} from "../scheduler/coloradoTracerRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url", "--year"]);

export function parseColoradoTracerRawDataRefreshTriggerArgs(
  args: readonly string[]
): ColoradoTracerRawDataRefreshJobData {
  assertKnownCliFlags(args, "Colorado TRACER raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    year: readStrictPositiveIntegerFlag(args, "--year"),
    url: readStrictFlagValue(args, "--url") || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseColoradoTracerRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualColoradoTracerRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("Colorado TRACER raw-data refresh is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Colorado TRACER raw-data refresh job enqueued (jobId=${jobId} force=${Boolean(jobData.force)})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Colorado TRACER raw-data refresh trigger failed:", error);
    process.exit(1);
  });
}
