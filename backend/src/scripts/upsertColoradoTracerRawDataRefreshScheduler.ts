import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isColoradoCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringColoradoTracerRawDataRefreshJobs,
  type ColoradoTracerRawDataRefreshJobData,
} from "../scheduler/coloradoTracerRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url", "--year"]);

export function parseUpsertColoradoTracerRawDataRefreshSchedulerArgs(
  args: readonly string[]
): ColoradoTracerRawDataRefreshJobData {
  assertKnownCliFlags(args, "Colorado TRACER raw data refresh scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
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
  const jobData = parseUpsertColoradoTracerRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isColoradoCampaignFinanceEnabled();
  await upsertRecurringColoradoTracerRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "Colorado TRACER raw-data refresh recurring scheduler upserted (daily metadata check)"
      : "Colorado TRACER raw-data refresh recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Colorado TRACER raw-data refresh scheduler upsert failed:", error);
    process.exit(1);
  });
}
