import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isPennsylvaniaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringPennsylvaniaCandidateFinanceSyncJobs,
  type PennsylvaniaCandidateFinanceSyncJobData,
} from "../scheduler/pennsylvaniaCandidateFinanceSyncScheduler.js";
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
  if (value.length === 0) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--raw-cache-dir", "--raw-extracted-dir", "--stale-after-days"]);

export function parseUpsertPennsylvaniaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): PennsylvaniaCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Pennsylvania candidate finance sync scheduler", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawDataExtractedDir: parseFlagValue(args, "--raw-extracted-dir") || undefined,
    rawDataCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertPennsylvaniaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isPennsylvaniaCampaignFinanceEnabled();
  await upsertRecurringPennsylvaniaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Pennsylvania campaign finance recurring scheduler upserted (daily sync)"
      : "Pennsylvania campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Pennsylvania campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
