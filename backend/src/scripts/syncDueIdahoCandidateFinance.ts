import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isIdahoCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueIdahoCandidateFinance,
  type IdahoCandidateFinanceBatchSyncResult,
} from "../pipeline/idahoFinance/idahoCandidateFinanceBatchSync.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";
import { parseIdahoFinancePositiveIntegerFlag } from "./idahoCandidateFinanceCliArgs.js";

export type SyncDueIdahoCandidateFinanceScriptOptions = {
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

export function parseSyncDueIdahoCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueIdahoCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Idaho candidate finance due sync", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    autoLinkMissingLinks: !args.includes("--no-auto-link"),
    maxCandidates: parseIdahoFinancePositiveIntegerFlag(args, "--max-candidates", undefined),
    staleAfterDays: parseIdahoFinancePositiveIntegerFlag(args, "--stale-after-days", undefined),
    electionLookbackDays: parseIdahoFinancePositiveIntegerFlag(args, "--lookback-days", undefined),
    electionLookaheadDays: parseIdahoFinancePositiveIntegerFlag(args, "--lookahead-days", undefined),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Idaho candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueIdahoCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueIdahoCandidateFinanceScriptOptions;
  result: IdahoCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "idaho_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueIdahoCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isIdahoCampaignFinanceSyncEnabled(options.force)) {
    console.log("Idaho campaign finance due sync disabled; no Idaho data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const result = await syncDueIdahoCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      autoLinkMissingLinks: options.autoLinkMissingLinks,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });
    console.log(JSON.stringify(toSyncDueIdahoCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
    // A link that failed is reported inside the JSON; the exit code must say
    // so too, or a scheduled run reads as green (North Dakota convention).
    if (result.failed > 0) {
      process.exitCode = 1;
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Idaho candidate finance due sync failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
