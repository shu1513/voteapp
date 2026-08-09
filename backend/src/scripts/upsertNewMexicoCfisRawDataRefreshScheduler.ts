import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isNewMexicoCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringNewMexicoCfisRawDataRefreshJobs,
  type NewMexicoCfisRawDataRefreshJobData,
} from "../scheduler/newMexicoCfisRawDataRefreshScheduler.js";
import type { NewMexicoCfisArtifactKind } from "../pipeline/newMexicoFinance/newMexicoCfisArtifactCache.js";
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

function parseArtifactKindFlag(args: readonly string[]): NewMexicoCfisArtifactKind | undefined {
  const raw = parseFlagValue(args, "--artifact-kind");
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
    year: parsePositiveIntegerFlag(args, "--year"),
    artifactKind: parseArtifactKindFlag(args),
    url: parseFlagValue(args, "--url") || undefined,
    cacheDir: parseFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
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
