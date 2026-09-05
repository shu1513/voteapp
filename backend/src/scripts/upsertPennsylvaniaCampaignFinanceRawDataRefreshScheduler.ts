import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isPennsylvaniaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringPennsylvaniaCampaignFinanceRawDataRefreshJobs,
  type PennsylvaniaCampaignFinanceRawDataRefreshJobData,
} from "../scheduler/pennsylvaniaCampaignFinanceRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url", "--year"]);

export function parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs(
  args: readonly string[]
): PennsylvaniaCampaignFinanceRawDataRefreshJobData {
  assertKnownCliFlags(args, "Pennsylvania campaign finance raw data refresh scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
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
  const jobData = parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isPennsylvaniaCampaignFinanceEnabled();
  await upsertRecurringPennsylvaniaCampaignFinanceRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "Pennsylvania campaign finance raw-data refresh recurring scheduler upserted (daily metadata check)"
      : "Pennsylvania campaign finance raw-data refresh recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Pennsylvania campaign finance raw-data refresh scheduler upsert failed:", error);
    process.exit(1);
  });
}
