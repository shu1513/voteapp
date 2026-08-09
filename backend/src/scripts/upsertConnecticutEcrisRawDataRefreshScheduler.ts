import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isConnecticutCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringConnecticutEcrisRawDataRefreshJobs,
  type ConnecticutEcrisRawDataRefreshJobData,
} from "../scheduler/connecticutEcrisRawDataRefreshScheduler.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(inlinePrefix));
  if (inline) {
    const value = inline.slice(inlinePrefix.length).trim();
    if (value.length === 0) {
      throw new Error(`Missing ${name} value`);
    }
    return value;
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const next = args[index + 1];
    if (!next || next.startsWith("--") || next.trim().length === 0) {
      throw new Error(`Missing ${name} value`);
    }
    return next.trim();
  }

  return null;
}

function parsePositiveIntegerFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parseEnumFlag<T extends string>(
  args: readonly string[],
  name: string,
  allowed: readonly T[]
): T | undefined {
  const raw = parseFlagValue(args, name);
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

export function parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs(
  args: readonly string[]
): ConnecticutEcrisRawDataRefreshJobData {
  assertKnownCliFlags(args, "Connecticut eCRIS raw data refresh scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    year: parsePositiveIntegerFlag(args, "--year"),
    transactionType: parseEnumFlag(args, "--transaction-type", ["receipts", "disbursements"]),
    committeeType: parseEnumFlag(args, "--committee-type", ["candidate_exploratory", "party_pac"]),
    period: parseEnumFlag(args, "--period", ["election", "calendar"]),
    format: parseEnumFlag(args, "--format", ["csv", "xlsx", "xls"]),
    url: parseFlagValue(args, "--url") || undefined,
    cacheDir: parseFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isConnecticutCampaignFinanceEnabled();
  await upsertRecurringConnecticutEcrisRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "Connecticut eCRIS raw-data refresh recurring scheduler upserted (daily metadata check)"
      : "Connecticut eCRIS raw-data refresh recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Connecticut eCRIS raw-data refresh scheduler upsert failed:", error);
    process.exit(1);
  });
}
