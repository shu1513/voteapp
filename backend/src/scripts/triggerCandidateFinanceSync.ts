import { pathToFileURL } from "node:url";

import {
  enqueueManualCandidateFinanceSyncJob,
  type CandidateFinanceSyncJobData,
} from "../scheduler/candidateFinanceSyncScheduler.js";
import { loadProjectEnv } from "../config/env.js";

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

function parseOptionalStringFlag(args: readonly string[], name: string): string | undefined {
  const value = parseFlagValue(args, name)?.trim();
  return value && value.length > 0 ? value : undefined;
}

export function parseCandidateFinanceSyncTriggerArgs(args: readonly string[]): CandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    includeOutside: args.includes("--include-outside"),
    candidateId: parseOptionalStringFlag(args, "--candidate-id"),
    fecCandidateId: parseOptionalStringFlag(args, "--fec-id"),
    electionYear: parsePositiveIntegerFlag(args, "--year"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    perPage: parsePositiveIntegerFlag(args, "--per-page"),
    outsideGroupLimit: parsePositiveIntegerFlag(args, "--top-groups"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualCandidateFinanceSyncJob(jobData);
  console.log(
    `candidate_finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )} includeOutside=${Boolean(jobData.includeOutside)} fecCandidateId=${jobData.fecCandidateId ?? "batch"} year=${
      jobData.electionYear ?? "batch"
    })`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("candidate_finance sync trigger failed:", error);
    process.exit(1);
  });
}
