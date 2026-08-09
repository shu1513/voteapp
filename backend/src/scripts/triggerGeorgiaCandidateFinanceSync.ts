import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualGeorgiaCandidateFinanceSyncJob,
  type GeorgiaCandidateFinanceSyncJobData,
} from "../scheduler/georgiaCandidateFinanceSyncScheduler.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
]);

// Strict like syncDueGeorgiaCandidateFinance: an unknown flag (e.g. the
// typo --dryrun) must fail loudly instead of silently enqueueing a real
// write, and a repeated flag must not quietly take one of its values.
function validateKnownFlags(args: readonly string[]): void {
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!KNOWN_BOOLEAN_FLAGS.has(name) && !KNOWN_VALUE_FLAGS.has(name)) {
      throw new Error(`Unknown Georgia candidate finance sync flag: ${name}`);
    }
    // --dry-run=true would pass the name check yet fail the later
    // args.includes("--dry-run") test and run a REAL sync — boolean flags
    // never take a value.
    if (KNOWN_BOOLEAN_FLAGS.has(name) && arg.includes("=")) {
      throw new Error(`Boolean flag does not accept a value: ${name}`);
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

export function parseGeorgiaCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): GeorgiaCandidateFinanceSyncJobData {
  validateKnownFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseGeorgiaCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualGeorgiaCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Georgia campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Georgia campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(
      jobData.force
    )} dryRun=${Boolean(jobData.dryRun)})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Georgia campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
