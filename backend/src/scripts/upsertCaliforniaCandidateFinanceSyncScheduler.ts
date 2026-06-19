import { loadProjectEnv } from "../config/env.js";
import { isCaliforniaCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringCaliforniaCandidateFinanceSyncJobs,
  type CaliforniaCandidateFinanceSyncJobData,
} from "../scheduler/californiaCandidateFinanceSyncScheduler.js";

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(inlinePrefix));
  if (inline) {
    return inline.slice(inlinePrefix.length);
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const next = args[index + 1];
    if (next && !next.startsWith("--")) {
      return next;
    }
  }

  return null;
}

function parsePositiveIntegerFlag(args: readonly string[], name: string): number | undefined {
  const value = parseFlagValue(args, name)?.trim();
  if (!value) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

function parseJobData(args: readonly string[]): CaliforniaCandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    includeOutside: !args.includes("--skip-outside"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseJobData(process.argv.slice(2));
  const enabled = isCaliforniaCampaignFinanceEnabled();
  await upsertRecurringCaliforniaCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "California campaign finance recurring scheduler upserted (daily sync)"
      : "California campaign finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

main().catch((error) => {
  console.error("California campaign finance recurring scheduler upsert failed:", error);
  process.exit(1);
});
