import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isNorthCarolinaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueNorthCarolinaCandidateFinance,
  type NorthCarolinaCandidateFinanceBatchSyncResult,
} from "../pipeline/northCarolinaFinance/northCarolinaCandidateFinanceBatchSync.js";

export type SyncDueNorthCarolinaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawCacheDir?: string;
};

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--raw-cache-dir",
]);

// Strict flag validation (ohio pattern): an unknown flag (e.g. the typo
// --dryrun) must fail loudly instead of silently running a real sync.
function validateKnownFlags(args: readonly string[]): void {
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!KNOWN_BOOLEAN_FLAGS.has(name) && !KNOWN_VALUE_FLAGS.has(name)) {
      throw new Error(`Unknown North Carolina candidate finance due sync flag: ${name}`);
    }
    // --dry-run=true would pass the name check yet fail the later
    // args.includes("--dry-run") test and run a REAL sync — boolean flags
    // never take a value.
    if (KNOWN_BOOLEAN_FLAGS.has(name) && arg.includes("=")) {
      throw new Error(`Boolean flag does not accept a value: ${name}`);
    }
  }
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

export function parseSyncDueNorthCarolinaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueNorthCarolinaCandidateFinanceScriptOptions {
  validateKnownFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    rawCacheDir: parseFlagValue(args, "--raw-cache-dir") || undefined,
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for North Carolina candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueNorthCarolinaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueNorthCarolinaCandidateFinanceScriptOptions;
  result: NorthCarolinaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "north_carolina_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueNorthCarolinaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isNorthCarolinaCampaignFinanceSyncEnabled(options.force)) {
    console.log("North Carolina campaign finance due sync disabled; no North Carolina data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueNorthCarolinaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      rawDataCacheDir: options.rawCacheDir,
    });

    console.log(
      JSON.stringify(toSyncDueNorthCarolinaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2)
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("North Carolina candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
