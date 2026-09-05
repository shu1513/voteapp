import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualConnecticutEcrisRawDataRefreshJob,
  type ConnecticutEcrisRawDataRefreshJobData,
} from "../scheduler/connecticutEcrisRawDataRefreshScheduler.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

function parseEnumFlag<T extends string>(
  args: readonly string[],
  name: string,
  allowed: readonly T[]
): T | undefined {
  const raw = readStrictFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!allowed.includes(raw as T)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return raw as T;
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--committee-type", "--format", "--period", "--timeout-ms", "--transaction-type", "--url", "--year"]);

export function parseConnecticutEcrisRawDataRefreshTriggerArgs(
  args: readonly string[]
): ConnecticutEcrisRawDataRefreshJobData {
  assertKnownCliFlags(args, "Connecticut eCRIS raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    year: readStrictPositiveIntegerFlag(args, "--year"),
    transactionType: parseEnumFlag(args, "--transaction-type", ["receipts", "disbursements"]),
    committeeType: parseEnumFlag(args, "--committee-type", ["candidate_exploratory", "party_pac"]),
    period: parseEnumFlag(args, "--period", ["election", "calendar"]),
    format: parseEnumFlag(args, "--format", ["csv", "xlsx", "xls"]),
    url: readStrictFlagValue(args, "--url") || undefined,
    cacheDir: readStrictFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseConnecticutEcrisRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualConnecticutEcrisRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("Connecticut eCRIS raw-data refresh is disabled; job was not enqueued");
    return;
  }
  console.log(`Connecticut eCRIS raw-data refresh job enqueued (jobId=${jobId} force=${Boolean(jobData.force)})`);
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Connecticut eCRIS raw-data refresh trigger failed:", error);
    process.exit(1);
  });
}
