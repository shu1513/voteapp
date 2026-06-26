import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualOregonCandidateFinanceSyncJob,
  type OregonCandidateFinanceSyncJobData,
} from "../scheduler/oregonCandidateFinanceSyncScheduler.js";

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

export function parseOregonCandidateFinanceSyncTriggerArgs(args: readonly string[]): OregonCandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    directMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--direct-max-breakdowns"),
    outsideMaxGroups: parsePositiveIntegerFlag(args, "--outside-max-groups"),
    outsideMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--outside-max-breakdowns"),
    minIndustryAmount: parsePositiveIntegerFlag(args, "--min-industry-amount"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseOregonCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualOregonCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Oregon campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Oregon campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Oregon campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
