import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualTexasCandidateFinanceSyncJob,
  type TexasCandidateFinanceSyncJobData,
} from "../scheduler/texasCandidateFinanceSyncScheduler.js";

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

export function parseTexasCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): TexasCandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawDataZipPath: parseFlagValue(args, "--raw-zip") || undefined,
    rawDataCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseTexasCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualTexasCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Texas campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Texas campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(
      jobData.force
    )} dryRun=${Boolean(jobData.dryRun)})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Texas campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
