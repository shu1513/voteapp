import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualIndianaCampaignFinanceRawDataRefreshJob,
  type IndianaCampaignFinanceRawDataRefreshJobData,
} from "../scheduler/indianaCampaignFinanceRawDataRefreshScheduler.js";
import { normalizeIndianaCampaignFinanceArtifactKind } from "../pipeline/indianaFinance/indianaCampaignFinanceArtifactCache.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownIndianaCampaignFinanceCliArgs } from "./indianaCampaignFinanceCliArgs.js";

export function parseIndianaCampaignFinanceRawDataRefreshTriggerArgs(
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
  const jobData = parseIndianaCampaignFinanceRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualIndianaCampaignFinanceRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("Indiana campaign finance raw data refresh is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Indiana campaign finance raw data refresh job enqueued (jobId=${jobId} force=${Boolean(
      jobData.force
    )} year=${jobData.year ?? "current"} artifactKind=${jobData.artifactKind ?? "contribution"})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Indiana campaign finance raw data refresh trigger failed:", error);
    process.exit(1);
  });
}
