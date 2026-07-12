import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isIllinoisCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringIllinoisCandidateFinanceSyncJobs,
  type IllinoisCandidateFinanceSyncJobData,
} from "../scheduler/illinoisCandidateFinanceSyncScheduler.js";
import { parseFlagValue, parseFlagValues } from "./illinoisCandidateFinanceScriptArgs.js";

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

function assertNoUnknownFlags(args: readonly string[], allowedFlags: readonly string[]): void {
  const allowed = new Set(allowedFlags);
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const flag = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!allowed.has(flag)) {
      throw new Error(`Unknown option: ${flag}`);
    }
  }
}

function assertBareBooleanFlags(args: readonly string[], booleanFlags: readonly string[]): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg || !booleanFlags.includes(arg)) {
      continue;
    }
    const next = args[index + 1];
    if (next !== undefined && !next.startsWith("--")) {
      throw new Error(`Boolean flag ${arg} does not take a value`);
    }
  }
  for (const flag of booleanFlags) {
    if (args.some((arg) => arg.startsWith(`${flag}=`))) {
      throw new Error(`Boolean flag ${flag} does not take a value`);
    }
  }
}

function assertMutuallyExclusiveFlags(args: readonly string[], left: string, right: string): void {
  if (args.includes(left) && args.includes(right)) {
    throw new Error(`Provide ${left} or ${right}, not both`);
  }
}

export function parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): IllinoisCandidateFinanceSyncJobData {
  assertNoUnknownFlags(args, [
    "--dry-run",
    "--force",
    "--max-candidates",
    "--stale-after-days",
    "--lookback-days",
    "--lookahead-days",
    "--ai-classify-industries",
    "--no-ai-classify-industries",
    "--ai-min-amount",
    "--contributions-csv",
    "--expenditures-csv",
    "--contributions-url",
    "--expenditures-url",
    "--normalized-artifact",
  ]);
  assertBareBooleanFlags(args, ["--dry-run", "--force", "--ai-classify-industries", "--no-ai-classify-industries"]);
  assertMutuallyExclusiveFlags(args, "--ai-classify-industries", "--no-ai-classify-industries");
  const contributionCsvPaths = parseFlagValues(args, "--contributions-csv");
  const expenditureCsvPaths = parseFlagValues(args, "--expenditures-csv");
  const contributionSourceUrl = parseFlagValue(args, "--contributions-url") || undefined;
  const expenditureSourceUrl = parseFlagValue(args, "--expenditures-url") || undefined;
  const normalizedArtifactPath = parseFlagValue(args, "--normalized-artifact") || undefined;
  if (
    contributionCsvPaths.length === 0 &&
    (expenditureCsvPaths.length > 0 || contributionSourceUrl || expenditureSourceUrl)
  ) {
    throw new Error("Provide --contributions-csv when using Illinois SBE artifact flags");
  }
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    aiClassifyIndustries: args.includes("--ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
    contributionCsvPaths: contributionCsvPaths.length > 0 ? contributionCsvPaths : undefined,
    expenditureCsvPaths: expenditureCsvPaths.length > 0 ? expenditureCsvPaths : undefined,
    contributionSourceUrl,
    expenditureSourceUrl,
    normalizedArtifactPath,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isIllinoisCampaignFinanceSyncEnabled(Boolean(jobData.force));
  await upsertRecurringIllinoisCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Illinois campaign finance recurring scheduler upserted (daily sync)"
      : "Illinois campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Illinois campaign finance recurring scheduler upsert failed:", error);
    process.exitCode = 1;
  });
}
