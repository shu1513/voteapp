import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isNewHampshireCfsRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  syncDueNewHampshireCandidateFinance,
  type NewHampshireCandidateFinanceBatchSyncResult,
} from "../pipeline/newHampshireFinance/newHampshireCandidateFinanceBatchSync.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";
import { parseNewHampshireFinancePositiveIntegerFlag } from "./newHampshireCandidateFinanceCliArgs.js";

export type SyncDueNewHampshireCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  autoLinkMissingLinks: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
};

const BOOLEAN_FLAGS = new Set(["--dry-run", "--force", "--no-auto-link"]);
const VALUE_FLAGS = new Set(["--max-candidates", "--stale-after-days", "--lookback-days", "--lookahead-days"]);

export function parseSyncDueNewHampshireCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueNewHampshireCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "New Hampshire candidate finance due sync", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    autoLinkMissingLinks: !args.includes("--no-auto-link"),
    maxCandidates: parseNewHampshireFinancePositiveIntegerFlag(args, "--max-candidates", undefined),
    staleAfterDays: parseNewHampshireFinancePositiveIntegerFlag(args, "--stale-after-days", undefined),
    electionLookbackDays: parseNewHampshireFinancePositiveIntegerFlag(args, "--lookback-days", undefined),
    electionLookaheadDays: parseNewHampshireFinancePositiveIntegerFlag(args, "--lookahead-days", undefined),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for New Hampshire candidate finance due sync");
  }
  return databaseUrl;
}

/**
 * A run is green only when every due link synced AND the auto-link pass
 * neither threw nor left a candidate in error: a CFS or database outage that
 * created zero links must not read as success to a scheduled runner.
 */
export function syncDueNewHampshireCandidateFinanceExitCode(
  result: NewHampshireCandidateFinanceBatchSyncResult
): 0 | 1 {
  const autoLinkErrors = result.autoLinkResults.filter((entry) => entry.status === "error").length;
  return result.failed > 0 || result.autoLinkError !== null || autoLinkErrors > 0 ? 1 : 0;
}

export function toSyncDueNewHampshireCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueNewHampshireCandidateFinanceScriptOptions;
  result: NewHampshireCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "new_hampshire_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueNewHampshireCandidateFinanceScriptArgs(process.argv.slice(2));

  // The sync reads the live CFS search API, so it shares the live-call gate
  // (the master flag plus the raw-refresh flag, or --force).
  if (!isNewHampshireCfsRawDataRefreshEnabled(options.force)) {
    console.log("New Hampshire campaign finance due sync disabled; no New Hampshire data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const result = await syncDueNewHampshireCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      autoLinkMissingLinks: options.autoLinkMissingLinks,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });
    console.log(
      JSON.stringify(toSyncDueNewHampshireCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2)
    );
    // Failures are reported inside the JSON; the exit code must say so too,
    // or a scheduled run reads as green (North Dakota convention).
    const exitCode = syncDueNewHampshireCandidateFinanceExitCode(result);
    if (exitCode !== 0) {
      process.exitCode = exitCode;
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error(
      "New Hampshire candidate finance due sync failed:",
      error instanceof Error ? error.message : String(error)
    );
    process.exitCode = 1;
  });
}
