import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isKentuckyCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringKentuckyCandidateFinanceSyncJobs,
  type KentuckyCandidateFinanceSyncJobData,
} from "../scheduler/kentuckyCandidateFinanceSyncScheduler.js";

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

function parseAutoLinkMissingLinksFlag(args: readonly string[]): boolean {
  const enabled = args.includes("--auto-link");
  const disabled = args.includes("--no-auto-link");
  if (enabled && disabled) {
    throw new Error("Provide either --auto-link or --no-auto-link, not both");
  }
  return !disabled;
}

function assertNoUnknownFlags(args: readonly string[], supportedFlags: ReadonlySet<string>): void {
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!supportedFlags.has(name)) {
      throw new Error(`Unknown Kentucky candidate finance scheduler upsert flag: ${name}`);
    }
  }
}

export function parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): KentuckyCandidateFinanceSyncJobData {
  assertNoUnknownFlags(
    args,
    new Set([
      "--dry-run",
      "--force",
      "--max-candidates",
      "--stale-after-days",
      "--lookback-days",
      "--lookahead-days",
      "--auto-link",
      "--no-auto-link",
    ])
  );
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    autoLinkMissingLinks: parseAutoLinkMissingLinksFlag(args),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isKentuckyCampaignFinanceEnabled();
  await upsertRecurringKentuckyCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Kentucky campaign finance recurring scheduler upserted (daily sync)"
      : "Kentucky campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Kentucky campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
