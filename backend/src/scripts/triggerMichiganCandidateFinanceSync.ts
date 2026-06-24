import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualMichiganCandidateFinanceSyncJob,
  type MichiganCandidateFinanceSyncJobData,
} from "../scheduler/michiganCandidateFinanceSyncScheduler.js";

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
  const value = raw.trim();
  if (value.length === 0) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

export function parseMichiganCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): MichiganCandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawDataExtractedDir: parseFlagValue(args, "--raw-extracted-dir") || undefined,
    rawDataCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseMichiganCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualMichiganCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Michigan campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Michigan campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(
      jobData.force
    )} dryRun=${Boolean(jobData.dryRun)})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Michigan campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
