import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  isNebraskaCampaignFinanceEnabled,
  isNebraskaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  upsertRecurringNebraskaCandidateFinanceSyncJobs,
  type NebraskaCandidateFinanceSyncJobData,
} from "../scheduler/nebraskaCandidateFinanceSyncScheduler.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--raw-cache-dir",
  "--raw-zip",
]);

function assertNoUnknownNebraskaFinanceSchedulerArgs(args: readonly string[]): void {
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const name = arg.split("=", 1)[0] ?? arg;
    if (arg.includes("=") && KNOWN_BOOLEAN_FLAGS.has(name)) {
      throw new Error(`Boolean flag must not include a value: ${name}`);
    }
    if (!KNOWN_BOOLEAN_FLAGS.has(name) && !KNOWN_VALUE_FLAGS.has(name)) {
      throw new Error(`Unknown Nebraska candidate finance scheduler upsert flag: ${name}`);
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

export function parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs(
  args: readonly string[]
): NebraskaCandidateFinanceSyncJobData {
  assertNoUnknownNebraskaFinanceSchedulerArgs(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawDataCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
    rawDataZipPath: parseFlagValue(args, "--raw-zip") || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs(process.argv.slice(2));
  const enabled = isNebraskaCampaignFinanceEnabled() && isNebraskaCampaignFinanceSyncEnabled();
  await upsertRecurringNebraskaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "Nebraska campaign finance recurring scheduler upserted (daily sync)"
      : "Nebraska campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Nebraska campaign finance recurring scheduler upsert failed:", error);
    process.exit(1);
  });
}
