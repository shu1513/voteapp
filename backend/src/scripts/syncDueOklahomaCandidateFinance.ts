import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isOklahomaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueOklahomaCandidateFinance,
  type OklahomaCandidateFinanceBatchSyncResult,
} from "../pipeline/oklahomaFinance/oklahomaCandidateFinanceBatchSync.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type SyncDueOklahomaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawCacheDir?: string;
  rawZipPath?: string;
};

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--raw-cache-dir", "--raw-zip", "--stale-after-days"]);

export function parseSyncDueOklahomaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueOklahomaCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Oklahoma candidate finance due sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: readStrictPositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: readStrictPositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: readStrictPositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: readStrictPositiveIntegerFlag(args, "--lookahead-days"),
    rawCacheDir: readStrictFlagValue(args, "--raw-cache-dir") || undefined,
    rawZipPath: readStrictFlagValue(args, "--raw-zip") || undefined,
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Oklahoma candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueOklahomaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueOklahomaCandidateFinanceScriptOptions;
  result: OklahomaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "oklahoma_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueOklahomaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isOklahomaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Oklahoma campaign finance due sync disabled; no Oklahoma data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueOklahomaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      rawDataCacheDir: options.rawCacheDir,
      rawDataZipPath: options.rawZipPath,
    });

    console.log(JSON.stringify(toSyncDueOklahomaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Oklahoma candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
