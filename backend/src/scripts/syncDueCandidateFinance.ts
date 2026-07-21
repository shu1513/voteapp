import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isCandidateFinanceEnabled } from "../config/featureFlags.js";
import {
  syncDueCandidateFinance,
  type CandidateFinanceBatchSyncResult,
} from "../pipeline/finance/candidateFinanceBatchSync.js";
import { DEFAULT_OPEN_FEC_TIMEOUT_MS, readOpenFecApiKeysFromEnv } from "../pipeline/presidential/openFecClient.js";

export type SyncDueCandidateFinanceScriptOptions = {
  dryRun: boolean;
  includeOutside: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  perPage?: number;
  outsideGroupLimit?: number;
  timeoutMs?: number;
};

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(inlinePrefix));
  if (inline) {
    return inline.slice(inlinePrefix.length);
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const next = args[index + 1];
    if (next && !next.startsWith("--")) {
      return next;
    }
  }

  return null;
}

function parsePositiveIntegerFlag(args: readonly string[], name: string): number | undefined {
  const value = parseFlagValue(args, name)?.trim();
  if (!value) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

export function parseSyncDueCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueCandidateFinanceScriptOptions {
  return {
    dryRun: args.includes("--dry-run"),
    includeOutside: args.includes("--include-outside"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    perPage: parsePositiveIntegerFlag(args, "--per-page"),
    outsideGroupLimit: parsePositiveIntegerFlag(args, "--top-groups"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
  };
}

function getDatabaseUrl(): string {
  return process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp";
}

export function toSyncDueCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueCandidateFinanceScriptOptions;
  result: CandidateFinanceBatchSyncResult;
}) {
  return {
    type: "candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    include_outside: input.options.includeOutside,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueCandidateFinanceScriptArgs(process.argv.slice(2));
  if (!isCandidateFinanceEnabled()) {
    console.log("candidate_finance due sync disabled; no FEC data fetched");
    return;
  }

  const apiKeys = readOpenFecApiKeysFromEnv();
  const timeoutMs = options.timeoutMs ?? DEFAULT_OPEN_FEC_TIMEOUT_MS;

  if (apiKeys.length === 0) {
    throw new Error("No OpenFEC API keys configured. Set FEC_API_KEY_1 or FEC_API_KEY.");
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueCandidateFinance({
      db: pool,
      openFecOptions: { apiKeys, timeoutMs },
      now: startedAt,
      dryRun: options.dryRun,
      includeOutside: options.includeOutside,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      perPage: options.perPage,
      outsideGroupLimit: options.outsideGroupLimit,
    });

    console.log(JSON.stringify(toSyncDueCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
