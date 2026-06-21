import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isNebraskaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueNebraskaCandidateFinance,
  type NebraskaCandidateFinanceBatchSyncResult,
} from "../pipeline/nebraskaFinance/nebraskaCandidateFinanceBatchSync.js";

export type SyncDueNebraskaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawCacheDir?: string;
  rawZipPath?: string;
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

export function parseSyncDueNebraskaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueNebraskaCandidateFinanceScriptOptions {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
    rawZipPath: parseFlagValue(args, "--raw-zip") || undefined,
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Nebraska candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueNebraskaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueNebraskaCandidateFinanceScriptOptions;
  result: NebraskaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "nebraska_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueNebraskaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isNebraskaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Nebraska campaign finance due sync disabled; no Nebraska data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueNebraskaCandidateFinance({
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

    console.log(JSON.stringify(toSyncDueNebraskaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Nebraska candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
