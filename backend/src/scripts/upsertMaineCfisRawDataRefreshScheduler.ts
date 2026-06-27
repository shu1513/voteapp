import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isMaineCampaignFinanceEnabled } from "../config/featureFlags.js";
import type { MaineCfisArtifactKind } from "../pipeline/maineFinance/maineCfisClient.js";
import {
  upsertRecurringMaineCfisRawDataRefreshJobs,
  type MaineCfisRawDataRefreshJobData,
} from "../scheduler/maineCfisRawDataRefreshScheduler.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--filing-year", "--year", "--artifact-kind", "--url", "--cache-dir", "--timeout-ms"]);

function validateKnownFlags(args: readonly string[]): void {
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!KNOWN_BOOLEAN_FLAGS.has(name) && !KNOWN_VALUE_FLAGS.has(name)) {
      throw new Error(`Unknown Maine CFIS raw data refresh scheduler upsert flag: ${name}`);
    }
  }
}

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || next.trim().length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(next.trim());
      index += 1;
    }
  }

  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0] ?? null;
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

function parseFilingYearFlag(args: readonly string[]): number | undefined {
  const filingYear = parsePositiveIntegerFlag(args, "--filing-year");
  const year = parsePositiveIntegerFlag(args, "--year");
  if (filingYear !== undefined && year !== undefined) {
    throw new Error("Provide --filing-year at most once");
  }
  return filingYear ?? year;
}

function parseArtifactKindFlag(args: readonly string[]): MaineCfisArtifactKind | undefined {
  const raw = parseFlagValue(args, "--artifact-kind");
  if (raw === null) {
    return undefined;
  }
  if (raw !== "contributions" && raw !== "expenditures") {
    throw new Error(`Invalid --artifact-kind value: ${raw}`);
  }
  return raw;
}

export function parseUpsertMaineCfisRawDataRefreshSchedulerArgs(
  args: readonly string[]
): MaineCfisRawDataRefreshJobData {
  validateKnownFlags(args);
  return {
    force: args.includes("--force"),
    filingYear: parseFilingYearFlag(args),
    artifactKind: parseArtifactKindFlag(args),
    url: parseFlagValue(args, "--url") || undefined,
    cacheDir: parseFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertMaineCfisRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isMaineCampaignFinanceEnabled();
  await upsertRecurringMaineCfisRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "Maine CFIS raw-data refresh recurring scheduler upserted (daily metadata check)"
      : "Maine CFIS raw-data refresh recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Maine CFIS raw-data refresh scheduler upsert failed:", error);
    process.exit(1);
  });
}
