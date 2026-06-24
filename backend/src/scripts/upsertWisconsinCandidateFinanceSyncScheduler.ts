import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isWisconsinCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringWisconsinCandidateFinanceSyncJobs,
  type WisconsinCandidateFinanceSyncJobData,
} from "../scheduler/wisconsinCandidateFinanceSyncScheduler.js";

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

export function parseUpsertWisconsinCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): WisconsinCandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertWisconsinCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isWisconsinCampaignFinanceEnabled();
  await upsertRecurringWisconsinCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Wisconsin campaign finance recurring scheduler upserted (daily sync)"
      : "Wisconsin campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Wisconsin campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
