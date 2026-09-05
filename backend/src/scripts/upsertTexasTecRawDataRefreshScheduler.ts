import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isTexasCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringTexasTecRawDataRefreshJobs,
  type TexasTecRawDataRefreshJobData,
} from "../scheduler/texasTecRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url"]);

export function parseUpsertTexasTecRawDataRefreshSchedulerArgs(
  args: readonly string[]
): TexasTecRawDataRefreshJobData {
  assertKnownCliFlags(args, "Texas TEC raw data refresh scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    url: readStrictFlagValue(args, "--url") || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertTexasTecRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isTexasCampaignFinanceEnabled();
  await upsertRecurringTexasTecRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "Texas TEC raw-data refresh recurring scheduler upserted (daily metadata check)"
      : "Texas TEC raw-data refresh recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Texas TEC raw-data refresh scheduler upsert failed:", error);
    process.exit(1);
  });
}
