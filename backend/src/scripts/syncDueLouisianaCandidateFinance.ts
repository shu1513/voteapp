import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isLouisianaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

const LOUISIANA_CANDIDATE_FINANCE_BATCH_SYNC_MODULE =
  "../pipeline/louisianaFinance/louisianaCandidateFinanceBatchSync.js";

export type SyncDueLouisianaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
};

export type LouisianaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  autoLinkAttemptedCount: number;
  autoLinkLinkedCount: number;
  results: unknown[];
};

type LouisianaCandidateFinanceBatchSyncModule = {
  syncDueLouisianaCandidateFinance: (input: {
    db: Pool;
    now: Date;
    dryRun: boolean;
    maxCandidates?: number;
    staleAfterDays?: number;
    electionLookbackDays?: number;
    electionLookaheadDays?: number;
    rawDataCacheDir?: string;
  }) => Promise<LouisianaCandidateFinanceBatchSyncResult>;
};

async function loadLouisianaCandidateFinanceBatchSyncModule(): Promise<LouisianaCandidateFinanceBatchSyncModule> {
  return (await import(LOUISIANA_CANDIDATE_FINANCE_BATCH_SYNC_MODULE)) as LouisianaCandidateFinanceBatchSyncModule;
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--lookahead-days", "--lookback-days", "--max-candidates", "--raw-cache-dir", "--stale-after-days"]);

function assertNoUnknownLouisianaFinanceArgs(args: readonly string[]): void {
  assertKnownCliFlags(args, "Louisiana candidate finance due sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
}

export function parseSyncDueLouisianaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueLouisianaCandidateFinanceScriptOptions {
  assertNoUnknownLouisianaFinanceArgs(args);

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

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Louisiana candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueLouisianaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueLouisianaCandidateFinanceScriptOptions;
  result: LouisianaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "louisiana_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueLouisianaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isLouisianaCampaignFinanceSyncEnabled(options.force)) {
    console.log(
      JSON.stringify(
        {
          type: "louisiana_candidate_finance_due_sync",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          dry_run: options.dryRun,
          enabled: false,
          reason: "disabled",
        },
        null,
        2
      )
    );
    return;
  }

  const { syncDueLouisianaCandidateFinance } = await loadLouisianaCandidateFinanceBatchSyncModule();
  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueLouisianaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      rawDataCacheDir: options.rawDataCacheDir,
    });

    console.log(JSON.stringify(toSyncDueLouisianaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Louisiana candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
