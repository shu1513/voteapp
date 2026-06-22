import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isOklahomaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringOklahomaCandidateFinanceSyncJobs,
  type OklahomaCandidateFinanceSyncJobData,
} from "../scheduler/oklahomaCandidateFinanceSyncScheduler.js";

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

export function parseUpsertOklahomaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): OklahomaCandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawDataCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertOklahomaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isOklahomaCampaignFinanceEnabled();
  await upsertRecurringOklahomaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Oklahoma campaign finance recurring scheduler upserted (daily sync)"
      : "Oklahoma campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Oklahoma campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
