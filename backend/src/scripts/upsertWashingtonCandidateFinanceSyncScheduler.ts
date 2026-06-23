import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isWashingtonCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringWashingtonCandidateFinanceSyncJobs,
  type WashingtonCandidateFinanceSyncJobData,
} from "../scheduler/washingtonCandidateFinanceSyncScheduler.js";

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

export function parseUpsertWashingtonCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): WashingtonCandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    aiClassifyIndustries: args.includes("--ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertWashingtonCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isWashingtonCampaignFinanceEnabled();
  await upsertRecurringWashingtonCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Washington campaign finance recurring scheduler upserted (daily sync)"
      : "Washington campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Washington campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
