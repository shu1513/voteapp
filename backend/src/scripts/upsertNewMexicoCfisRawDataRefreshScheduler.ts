import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isNewMexicoCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringNewMexicoCfisRawDataRefreshJobs,
  type NewMexicoCfisRawDataRefreshJobData,
} from "../scheduler/newMexicoCfisRawDataRefreshScheduler.js";
import type { NewMexicoCfisArtifactKind } from "../pipeline/newMexicoFinance/newMexicoCfisArtifactCache.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

function parseArtifactKindFlag(args: readonly string[]): NewMexicoCfisArtifactKind | undefined {
  const raw = readStrictFlagValue(args, "--artifact-kind");
  if (raw === null) {
    return undefined;
  }
  if (raw !== "contributions" && raw !== "expenditures") {
    throw new Error(`Invalid --artifact-kind value: ${raw}`);
  }
  return raw;
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--artifact-kind", "--cache-dir", "--timeout-ms", "--url", "--year"]);

export function parseUpsertNewMexicoCfisRawDataRefreshSchedulerArgs(
  args: readonly string[]
): NewMexicoCfisRawDataRefreshJobData {
  assertKnownCliFlags(args, "New Mexico CFIS raw data refresh scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    year: readStrictPositiveIntegerFlag(args, "--year"),
    artifactKind: parseArtifactKindFlag(args),
    url: readStrictFlagValue(args, "--url") || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertNewMexicoCfisRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isNewMexicoCampaignFinanceEnabled();
  await upsertRecurringNewMexicoCfisRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "New Mexico CFIS raw-data refresh recurring scheduler upserted (daily metadata check)"
      : "New Mexico CFIS raw-data refresh recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("New Mexico CFIS raw-data refresh scheduler upsert failed:", error);
    process.exit(1);
  });
}
