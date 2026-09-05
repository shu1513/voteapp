import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isIndianaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringIndianaCampaignFinanceRawDataRefreshJobs,
  type IndianaCampaignFinanceRawDataRefreshJobData,
} from "../scheduler/indianaCampaignFinanceRawDataRefreshScheduler.js";
import { normalizeIndianaCampaignFinanceArtifactKind } from "../pipeline/indianaFinance/indianaCampaignFinanceArtifactCache.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownIndianaCampaignFinanceCliArgs } from "./indianaCampaignFinanceCliArgs.js";

export function parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(
  args: readonly string[]
): IndianaCampaignFinanceRawDataRefreshJobData {
  assertKnownIndianaCampaignFinanceCliArgs(args, [
    { name: "--force", takesValue: false },
    { name: "--year", takesValue: true },
    { name: "--artifact-kind", takesValue: true },
    { name: "--url", takesValue: true },
    { name: "--cache-dir", takesValue: true },
    { name: "--timeout-ms", takesValue: true },
  ]);
  const artifactKind = readStrictFlagValue(args, "--artifact-kind");
  return {
    force: args.includes("--force"),
    year: readStrictPositiveIntegerFlag(args, "--year"),
    artifactKind: artifactKind ? normalizeIndianaCampaignFinanceArtifactKind(artifactKind) : undefined,
    url: readStrictFlagValue(args, "--url") || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isIndianaCampaignFinanceEnabled();
  await upsertRecurringIndianaCampaignFinanceRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "Indiana campaign finance raw data recurring scheduler upserted"
      : "Indiana campaign finance raw data recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Indiana campaign finance raw data recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
