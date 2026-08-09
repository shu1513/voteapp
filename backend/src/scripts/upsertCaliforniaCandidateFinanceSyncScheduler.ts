import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isCaliforniaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringCaliforniaCandidateFinanceSyncJobs,
  type CaliforniaCandidateFinanceSyncJobData,
} from "../scheduler/californiaCandidateFinanceSyncScheduler.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(inlinePrefix));
  if (inline) {
    return inline.slice(inlinePrefix.length);
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const next = args[index + 1];
    if (!next || next.startsWith("--")) {
      throw new Error(`Missing ${name} value`);
    }
    return next;
  }

  return null;
}

function parsePositiveIntegerFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  const value = raw.trim();
  if (value.length === 0) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force", "--skip-outside"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--raw-cache-dir", "--raw-zip", "--stale-after-days", "--timeout-ms"]);

export function parseUpsertCaliforniaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): CaliforniaCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "California candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    includeOutside: !args.includes("--skip-outside"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    rawDataZipPath: parseFlagValue(args, "--raw-zip")?.trim() || undefined,
    rawDataCacheDir: parseFlagValue(args, "--raw-cache-dir")?.trim() || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertCaliforniaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isCaliforniaCampaignFinanceEnabled();
  await upsertRecurringCaliforniaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "California campaign finance recurring scheduler upserted (daily sync)"
      : "California campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("California campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
