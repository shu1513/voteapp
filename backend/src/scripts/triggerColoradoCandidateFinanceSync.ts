import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualColoradoCandidateFinanceSyncJob,
  type ColoradoCandidateFinanceSyncJobData,
} from "../scheduler/coloradoCandidateFinanceSyncScheduler.js";

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(inlinePrefix));
  if (inline) {
    return inline.slice(inlinePrefix.length);
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const next = args[index + 1];
    if (!next || next.startsWith("--")) {
      throw new Error(`Missing ${name} value`);
    }
    return next;
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

export function parseColoradoCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): ColoradoCandidateFinanceSyncJobData {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawDataZipPath: parseFlagValue(args, "--raw-zip")?.trim() || undefined,
    rawDataCacheDir: parseFlagValue(args, "--raw-cache-dir")?.trim() || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseColoradoCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualColoradoCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Colorado campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Colorado campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(
      jobData.force
    )} dryRun=${Boolean(jobData.dryRun)})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Colorado campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
