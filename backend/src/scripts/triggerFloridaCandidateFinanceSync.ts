import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualFloridaCandidateFinanceSyncJob,
  type FloridaCandidateFinanceSyncJobData,
} from "../scheduler/floridaCandidateFinanceSyncScheduler.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force", "--refresh-export-artifacts"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--artifact-cache-dir",
  "--export-min-interval-ms",
  "--export-row-limit",
]);

function assertNoUnknownFloridaFinanceTriggerArgs(args: readonly string[]): void {
  assertKnownCliFlags(args, "Florida candidate finance sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
}

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (!value) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || !next.trim()) {
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

function parseIntegerFlag(input: { args: readonly string[]; name: string; allowZero?: boolean }): number | undefined {
  const raw = parseFlagValue(input.args, input.name);
  if (raw === null) {
    return undefined;
  }
  const pattern = input.allowZero === true ? /^(?:0|[1-9]\d*)$/ : /^[1-9]\d*$/;
  if (!pattern.test(raw)) {
    throw new Error(`Invalid ${input.name} value: ${raw}`);
  }
  return Number(raw);
}

export function parseFloridaCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): FloridaCandidateFinanceSyncJobData {
  assertNoUnknownFloridaFinanceTriggerArgs(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    refreshExportArtifacts: args.includes("--refresh-export-artifacts"),
    maxCandidates: parseIntegerFlag({ args, name: "--max-candidates" }),
    staleAfterDays: parseIntegerFlag({ args, name: "--stale-after-days", allowZero: true }),
    electionLookbackDays: parseIntegerFlag({ args, name: "--lookback-days", allowZero: true }),
    electionLookaheadDays: parseIntegerFlag({ args, name: "--lookahead-days", allowZero: true }),
    defaultArtifactCacheDir: parseFlagValue(args, "--artifact-cache-dir") || undefined,
    exportMinIntervalMs: parseIntegerFlag({ args, name: "--export-min-interval-ms", allowZero: true }),
    exportRowLimit: parseIntegerFlag({ args, name: "--export-row-limit" }),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseFloridaCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualFloridaCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Florida campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Florida campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Florida campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
