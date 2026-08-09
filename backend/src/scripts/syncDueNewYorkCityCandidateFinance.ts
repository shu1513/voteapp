import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isNewYorkCityCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { syncDueNewYorkCityCandidateFinance } from "../pipeline/newYorkCityFinance/newYorkCityCandidateFinanceBatchSync.js";

export type SyncDueNewYorkCityCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  cacheDir?: string;
};

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--cache-dir",
]);

// Strict flag validation (ohio pattern): an unknown flag (e.g. the typo
// --dryrun) must fail loudly instead of silently running a real write, and
// a repeated flag must not quietly take one of its values.
function validateKnownFlags(args: readonly string[]): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (!arg.startsWith("--")) {
      // A bare token is only legal as the value of the immediately
      // preceding space-form value flag. Anything else is a positional typo
      // (e.g. "dry-run" after npm's own "--" separator) that would
      // otherwise be ignored and run a REAL sync.
      const previous = index > 0 ? args[index - 1]! : undefined;
      if (previous === undefined || !KNOWN_VALUE_FLAGS.has(previous)) {
        throw new Error(`Unexpected positional argument: ${arg}`);
      }
      continue;
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!KNOWN_BOOLEAN_FLAGS.has(name) && !KNOWN_VALUE_FLAGS.has(name)) {
      throw new Error(`Unknown New York City candidate finance due sync flag: ${name}`);
    }
    // --dry-run=true would pass the name check yet fail the later
    // args.includes("--dry-run") test and run a REAL sync - boolean
    // flags never take a value.
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
  if (!/^[1-9]\d*$/.test(raw) || !Number.isSafeInteger(Number(raw))) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

export function parseSyncDueNewYorkCityCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueNewYorkCityCandidateFinanceScriptOptions {
  validateKnownFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    cacheDir: parseFlagValue(args, "--cache-dir") ?? undefined,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseSyncDueNewYorkCityCandidateFinanceScriptArgs(process.argv.slice(2));
  if (!isNewYorkCityCampaignFinanceSyncEnabled(options.force)) {
    console.log(JSON.stringify({ type: "new_york_city_candidate_finance_due_sync", enabled: false }));
    return;
  }
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) throw new Error("DATABASE_URL is required for NYC candidate finance sync");
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const result = await syncDueNewYorkCityCandidateFinance({
      db: pool,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      cacheDir: options.cacheDir,
    });
    console.log(JSON.stringify({ type: "new_york_city_candidate_finance_due_sync", enabled: true, result }, null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("NYC candidate finance due sync failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
