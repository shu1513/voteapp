import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualNewMexicoCfisRawDataRefreshJob,
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

export function parseNewMexicoCfisRawDataRefreshTriggerArgs(
  args: readonly string[]
): NewMexicoCfisRawDataRefreshJobData {
  assertKnownCliFlags(args, "New Mexico CFIS raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
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
  const jobData = parseNewMexicoCfisRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualNewMexicoCfisRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("New Mexico CFIS raw-data refresh is disabled; job was not enqueued");
    return;
  }
  console.log(`New Mexico CFIS raw-data refresh job enqueued (jobId=${jobId} force=${Boolean(jobData.force)})`);
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("New Mexico CFIS raw-data refresh trigger failed:", error);
    process.exit(1);
  });
}
