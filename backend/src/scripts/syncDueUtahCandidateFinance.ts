import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isUtahCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueUtahCandidateFinance,
  type UtahCandidateFinanceBatchSyncResult,
} from "../pipeline/utahFinance/utahCandidateFinanceBatchSync.js";

export type SyncDueUtahCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawCacheDir?: string;
  refreshCache: boolean;
};

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

export function parseSyncDueUtahCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueUtahCandidateFinanceScriptOptions {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
    refreshCache: args.includes("--refresh-cache"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Utah candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueUtahCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueUtahCandidateFinanceScriptOptions;
  result: UtahCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "utah_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    refresh_cache: input.options.refreshCache,
    result: input.result,
  };
}

export async function runSyncDueUtahCandidateFinanceScript(input: {
  startedAt: Date;
  options: SyncDueUtahCandidateFinanceScriptOptions;
}): Promise<ReturnType<typeof toSyncDueUtahCandidateFinanceScriptOutput> | null> {
  if (!isUtahCampaignFinanceSyncEnabled(input.options.force)) {
    return null;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const result = await syncDueUtahCandidateFinance({
      db: pool,
      now: input.startedAt,
      dryRun: input.options.dryRun,
      maxCandidates: input.options.maxCandidates,
      staleAfterDays: input.options.staleAfterDays,
      electionLookbackDays: input.options.electionLookbackDays,
      electionLookaheadDays: input.options.electionLookaheadDays,
      rawDataCacheDir: input.options.rawCacheDir,
      refreshCache: input.options.refreshCache,
    });
    return toSyncDueUtahCandidateFinanceScriptOutput({ startedAt: input.startedAt, options: input.options, result });
  } finally {
    await pool.end();
  }
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueUtahCandidateFinanceScriptArgs(process.argv.slice(2));
  const output = await runSyncDueUtahCandidateFinanceScript({ startedAt, options });
  if (!output) {
    console.log("Utah campaign finance due sync disabled; no Utah data loaded");
    return;
  }
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Utah candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
