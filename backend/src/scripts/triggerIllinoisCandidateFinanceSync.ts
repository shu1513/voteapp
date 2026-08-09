import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualIllinoisCandidateFinanceSyncJob,
  type IllinoisCandidateFinanceSyncJobData,
} from "../scheduler/illinoisCandidateFinanceSyncScheduler.js";
import { parseFlagValue, parseFlagValues } from "./illinoisCandidateFinanceScriptArgs.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

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

function assertBareBooleanFlags(args: readonly string[], booleanFlags: readonly string[]): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg || !booleanFlags.includes(arg)) {
      continue;
    }
    const next = args[index + 1];
    if (next !== undefined && !next.startsWith("--")) {
      throw new Error(`Boolean flag ${arg} does not take a value`);
    }
  }
  for (const flag of booleanFlags) {
    if (args.some((arg) => arg.startsWith(`${flag}=`))) {
      throw new Error(`Boolean flag ${flag} does not take a value`);
    }
  }
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--contributions-csv", "--contributions-url", "--expenditures-csv", "--expenditures-url", "--lookahead-days", "--lookback-days", "--max-candidates", "--normalized-artifact", "--stale-after-days"]);

export function parseIllinoisCandidateFinanceSyncTriggerArgs(
  args: readonly string[]
): IllinoisCandidateFinanceSyncJobData {
  assertKnownCliFlags(args, "Illinois candidate finance sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  assertBareBooleanFlags(args, ["--dry-run", "--force"]);
  const contributionCsvPaths = parseFlagValues(args, "--contributions-csv");
  const expenditureCsvPaths = parseFlagValues(args, "--expenditures-csv");
  const contributionSourceUrl = parseFlagValue(args, "--contributions-url") || undefined;
  const expenditureSourceUrl = parseFlagValue(args, "--expenditures-url") || undefined;
  const normalizedArtifactPath = parseFlagValue(args, "--normalized-artifact") || undefined;
  if (
    contributionCsvPaths.length === 0 &&
    !normalizedArtifactPath &&
    (expenditureCsvPaths.length > 0 || contributionSourceUrl || expenditureSourceUrl)
  ) {
    throw new Error("Provide --contributions-csv or --normalized-artifact when using Illinois SBE artifact flags");
  }
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    contributionCsvPaths: contributionCsvPaths.length > 0 ? contributionCsvPaths : undefined,
    expenditureCsvPaths: expenditureCsvPaths.length > 0 ? expenditureCsvPaths : undefined,
    contributionSourceUrl,
    expenditureSourceUrl,
    normalizedArtifactPath,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseIllinoisCandidateFinanceSyncTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualIllinoisCandidateFinanceSyncJob(jobData);
  if (jobId === "disabled") {
    console.log("Illinois campaign finance sync is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Illinois campaign finance sync job enqueued (jobId=${jobId} force=${Boolean(jobData.force)} dryRun=${Boolean(
      jobData.dryRun
    )})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Illinois campaign finance sync trigger failed:", error);
    process.exitCode = 1;
  });
}
