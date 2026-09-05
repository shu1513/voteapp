import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isCaliforniaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringCaliforniaCampaignFinanceRawDataRefreshJobs,
  type CaliforniaCampaignFinanceRawDataRefreshJobData,
} from "../scheduler/californiaCampaignFinanceRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url"]);

export function parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(
  args: readonly string[]
): CaliforniaCampaignFinanceRawDataRefreshJobData {
  assertKnownCliFlags(args, "California campaign finance raw data refresh scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    url: readStrictFlagValue(args, "--url")?.trim() || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir")?.trim() || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isCaliforniaCampaignFinanceEnabled();
  await upsertRecurringCaliforniaCampaignFinanceRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "California campaign finance raw-data refresh recurring scheduler upserted (daily metadata check)"
      : "California campaign finance raw-data refresh recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("California campaign finance raw-data refresh scheduler upsert failed:", error);
    process.exit(1);
  });
}
