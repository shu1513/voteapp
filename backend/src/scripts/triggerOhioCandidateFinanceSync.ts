import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import {
  enqueueManualOhioCandidateFinanceSyncJob,
  type OhioCandidateFinanceSyncJobData,
} from "../scheduler/ohioCandidateFinanceSyncScheduler.js";

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--raw-cache-dir",
]);

// Strict like syncDueOhioCandidateFinance: an unknown flag (e.g. the typo
// --dryrun) must fail loudly instead of silently enqueueing a real write,
// and a repeated flag must not quietly take one of its values.
function validateKnownFlags(args: readonly string[]): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (!arg.startsWith("--")) {
      // A bare token is only legal as the value of the immediately
      // preceding space-form value flag. Anything else is a positional typo
      // (e.g. "dry-run" after npm's own "--" separator) that would
      // otherwise be ignored and enqueue a REAL sync.
      const previous = index > 0 ? args[index - 1]! : undefined;
      if (previous === undefined || !KNOWN_VALUE_FLAGS.has(previous)) {
        throw new Error(`Unexpected positional argument: ${arg}`);
      }
      continue;
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!KNOWN_BOOLEAN_FLAGS.has(name) && !KNOWN_VALUE_FLAGS.has(name)) {
      throw new Error(`Unknown Ohio candidate finance sync flag: ${name}`);
    }
    // --dry-run=true would pass the name check yet fail the later
    // args.includes("--dry-run") test and run a REAL sync - boolean
    // flags never take a value.
    if (KNOWN_BOOLEAN_FLAGS.has(name) && arg.includes("=")) {
      throw new Error(`Boolean flag does not accept a value: ${name}`);
    }
  }
}

export function parseOhioCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): OhioCandidateFinanceSyncJobData {
  validateKnownFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
    rawDataCacheDir: readStrictFlagValue(args, "--raw-cache-dir") || undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseOhioCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualOhioCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Ohio campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Ohio campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Ohio campaign finance sync trigger failed:", error);
    process.exit(1);
  });
}
