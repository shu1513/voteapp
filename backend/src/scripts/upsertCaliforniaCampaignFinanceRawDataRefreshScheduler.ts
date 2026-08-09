import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isCaliforniaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringCaliforniaCampaignFinanceRawDataRefreshJobs,
  type CaliforniaCampaignFinanceRawDataRefreshJobData,
} from "../scheduler/californiaCampaignFinanceRawDataRefreshScheduler.js";
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
  const value = raw.trim();
  if (value.length === 0 || !/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(value);
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url"]);

export function parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(
  args: readonly string[]
): CaliforniaCampaignFinanceRawDataRefreshJobData {
  assertKnownCliFlags(args, "California campaign finance raw data refresh scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    url: parseFlagValue(args, "--url")?.trim() || undefined,
    cacheDir: parseFlagValue(args, "--cache-dir")?.trim() || undefined,
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
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
