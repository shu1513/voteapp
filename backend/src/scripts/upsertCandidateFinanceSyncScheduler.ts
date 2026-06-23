import { isCandidateFinanceEnabled } from "../config/featureFlags.js";
import { loadProjectEnv } from "../config/env.js";
import {
  upsertRecurringCandidateFinanceSyncJobs,
  type CandidateFinanceSyncJobData,
} from "../scheduler/candidateFinanceSyncScheduler.js";

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

function parseJobData(args: readonly string[]): CandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    includeOutside: args.includes("--include-outside"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    perPage: parsePositiveIntegerFlag(args, "--per-page"),
    outsideGroupLimit: parsePositiveIntegerFlag(args, "--top-groups"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseJobData(process.argv.slice(2));
  const enabled = isCandidateFinanceEnabled();
  await upsertRecurringCandidateFinanceSyncJobs(jobData);
  console.log(
    enabled
      ? "candidate_finance recurring scheduler upserted (daily sync)"
      : "candidate_finance recurring scheduler disabled; scheduler cleanup completed"
  );
}

main().catch((error) => {
  console.error("candidate_finance recurring scheduler upsert failed:", error);
  process.exit(1);
});
