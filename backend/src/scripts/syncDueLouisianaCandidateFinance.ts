import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isLouisianaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";

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

const LOUISIANA_FINANCE_KNOWN_FLAGS = new Set([
  "--dry-run",
  "--force",
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--raw-cache-dir",
]);

function assertNoUnknownLouisianaFinanceArgs(args: readonly string[]): void {
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const isKnown = [...LOUISIANA_FINANCE_KNOWN_FLAGS].some((flag) => arg === flag || arg.startsWith(`${flag}=`));
    if (!isKnown) {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
}

export function parseSyncDueLouisianaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueLouisianaCandidateFinanceScriptOptions {
  assertNoUnknownLouisianaFinanceArgs(args);

  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawDataCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
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
    console.log("Louisiana campaign finance due sync disabled; no Louisiana data loaded");
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
