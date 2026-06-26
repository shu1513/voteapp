import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isAlaskaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringAlaskaCandidateFinanceSyncJobs,
  type AlaskaCandidateFinanceSyncJobData,
} from "../scheduler/alaskaCandidateFinanceSyncScheduler.js";
import type { AlaskaApocDataSourceMode } from "../pipeline/alaskaFinance/alaskaApocDataSource.js";

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

function parseNonNegativeIntegerFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^(?:0|[1-9]\d*)$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parseDataSourceMode(args: readonly string[]): AlaskaApocDataSourceMode | undefined {
  const liveFlag = args.includes("--live");
  const csvFlag = args.includes("--csv");
  if (liveFlag && csvFlag) {
    throw new Error("Provide either --live or --csv, not both");
  }
  const rawMode = parseFlagValue(args, "--data-source");
  if (rawMode !== null) {
    if (liveFlag || csvFlag) {
      throw new Error("Provide --data-source or --live/--csv, not both");
    }
    if (rawMode !== "csv" && rawMode !== "live") {
      throw new Error(`Invalid --data-source value: ${rawMode}`);
    }
    return rawMode;
  }
  return liveFlag ? "live" : csvFlag ? "csv" : undefined;
}

function parseDryRun(args: readonly string[]): boolean {
  const dryRunFlag = args.includes("--dry-run");
  const writeFlag = args.includes("--write");
  if (dryRunFlag && writeFlag) {
    throw new Error("Provide either --dry-run or --write, not both");
  }
  return !writeFlag;
}

function parseAutoLink(args: readonly string[]): boolean {
  const autoLinkFlag = args.includes("--auto-link");
  const noAutoLinkFlag = args.includes("--no-auto-link");
  if (autoLinkFlag && noAutoLinkFlag) {
    throw new Error("Provide either --auto-link or --no-auto-link, not both");
  }
  return autoLinkFlag;
}

export function parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): AlaskaCandidateFinanceSyncJobData {
  return {
    dryRun: parseDryRun(args),
    force: args.includes("--force"),
    autoLinkMissingLinks: parseAutoLink(args),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    dataSourceMode: parseDataSourceMode(args),
    incomeCsvPath: parseFlagValue(args, "--income-csv") || undefined,
    independentExpendituresCsvPath: parseFlagValue(args, "--ie-expenditures-csv") || undefined,
    independentContributionsCsvPath: parseFlagValue(args, "--ie-contributions-csv") || undefined,
    incomeUrl: parseFlagValue(args, "--income-url") || undefined,
    independentExpendituresUrl: parseFlagValue(args, "--ie-expenditures-url") || undefined,
    independentContributionsUrl: parseFlagValue(args, "--ie-contributions-url") || undefined,
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    retryCount: parseNonNegativeIntegerFlag(args, "--retry-count"),
    retryDelayMs: parseNonNegativeIntegerFlag(args, "--retry-delay-ms"),
    requestSpacingMs: parseNonNegativeIntegerFlag(args, "--request-spacing-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isAlaskaCampaignFinanceEnabled();
  await upsertRecurringAlaskaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Alaska campaign finance recurring scheduler upserted (daily sync)"
      : "Alaska campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Alaska campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
