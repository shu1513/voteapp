import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isGeorgiaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueGeorgiaCandidateFinance,
  type GeorgiaCandidateFinanceBatchSyncResult,
} from "../pipeline/georgiaFinance/georgiaCandidateFinanceBatchSync.js";

export type SyncDueGeorgiaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  maxPasses?: number;
};

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--max-candidates", "--stale-after-days", "--lookback-days", "--lookahead-days", "--max-passes"]);

function parsePositiveIntegerFlag(args: readonly string[], flag: string): number | undefined {
  const index = args.indexOf(flag);
  if (index === -1) {
    return undefined;
  }
  const raw = args[index + 1];
  const parsed = Number(raw);
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} requires a positive integer, got: ${raw}`);
  }
  return parsed;
}

export function parseSyncDueGeorgiaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueGeorgiaCandidateFinanceScriptOptions {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (KNOWN_BOOLEAN_FLAGS.has(arg)) {
      continue;
    }
    if (KNOWN_VALUE_FLAGS.has(arg)) {
      index += 1;
      continue;
    }
    throw new Error(`Unknown Georgia candidate finance flag: ${arg}`);
  }
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    // Stability passes for the windowed transaction pulls: large committees
    // with churn between passes can need more than the default 4 to produce
    // two consecutive equal id-sets (live: 8 candidates stuck at 4). 1 can
    // never converge (stability = two consecutive equal passes), so the
    // floor is 2.
    maxPasses: parseMaxPassesFlag(args),
  };
}

function parseMaxPassesFlag(args: readonly string[]): number | undefined {
  const value = parsePositiveIntegerFlag(args, "--max-passes");
  if (value !== undefined && value < 2) {
    throw new Error(`Invalid --max-passes value: ${value} (stability needs at least 2)`);
  }
  return value;
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Georgia candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueGeorgiaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueGeorgiaCandidateFinanceScriptOptions;
  result: GeorgiaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "georgia_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueGeorgiaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isGeorgiaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Georgia campaign finance due sync disabled; no Georgia data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueGeorgiaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      maxPasses: options.maxPasses,
    });

    console.log(JSON.stringify(toSyncDueGeorgiaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Georgia candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
