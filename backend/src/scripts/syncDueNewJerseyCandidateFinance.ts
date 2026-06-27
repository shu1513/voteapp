import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isNewJerseyCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueNewJerseyCandidateFinance,
  type NewJerseyCandidateFinanceBatchSyncResult,
} from "../pipeline/newJerseyFinance/newJerseyCandidateFinanceBatchSync.js";
import {
  assertKnownNewJerseyCampaignFinanceFlags,
  parseNewJerseyCampaignFinanceBooleanFlag,
  parseNewJerseyCampaignFinancePositiveIntegerFlag,
} from "./newJerseyCandidateFinanceCliArgs.js";

export type SyncDueNewJerseyCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
};

const SYNC_DUE_NEW_JERSEY_CANDIDATE_FINANCE_FLAGS = new Set([
  "--dry-run",
  "--force",
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
]);
const SYNC_DUE_NEW_JERSEY_CANDIDATE_FINANCE_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
]);

export function parseSyncDueNewJerseyCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueNewJerseyCandidateFinanceScriptOptions {
  assertKnownNewJerseyCampaignFinanceFlags(
    args,
    SYNC_DUE_NEW_JERSEY_CANDIDATE_FINANCE_FLAGS,
    SYNC_DUE_NEW_JERSEY_CANDIDATE_FINANCE_VALUE_FLAGS
  );
  return {
    dryRun: parseNewJerseyCampaignFinanceBooleanFlag(args, "--dry-run"),
    force: parseNewJerseyCampaignFinanceBooleanFlag(args, "--force"),
    maxCandidates: parseNewJerseyCampaignFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseNewJerseyCampaignFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseNewJerseyCampaignFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseNewJerseyCampaignFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for New Jersey candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueNewJerseyCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueNewJerseyCandidateFinanceScriptOptions;
  result: NewJerseyCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "new_jersey_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueNewJerseyCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isNewJerseyCampaignFinanceSyncEnabled(options.force)) {
    console.log("New Jersey campaign finance due sync disabled; no New Jersey data fetched");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueNewJerseyCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });

    console.log(JSON.stringify(toSyncDueNewJerseyCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("New Jersey candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
