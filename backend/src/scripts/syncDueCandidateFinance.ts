import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isCandidateFinanceEnabled } from "../config/featureFlags.js";
import {
  syncDueCandidateFinance,
  type CandidateFinanceBatchSyncResult,
} from "../pipeline/finance/candidateFinanceBatchSync.js";
import {
  DEFAULT_OPEN_FEC_TIMEOUT_MS,
  createOpenFecRateLimiter,
  readOpenFecApiKeysFromEnv,
} from "../pipeline/presidential/openFecClient.js";

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
  requestIntervalMs?: number;
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

function parseIntegerFlag(args: readonly string[], name: string, minimum: number): number | undefined {
  const value = parseFlagValue(args, name)?.trim();
  if (!value) {
    return undefined;
  }
  if (!/^\d+$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return parsed;
}

export function parseSyncDueCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueCandidateFinanceScriptOptions {
  return {
    dryRun: args.includes("--dry-run"),
    includeOutside: args.includes("--include-outside"),
    maxCandidates: parseIntegerFlag(args, "--max-candidates", 1),
    staleAfterDays: parseIntegerFlag(args, "--stale-after-days", 1),
    electionLookbackDays: parseIntegerFlag(args, "--lookback-days", 1),
    electionLookaheadDays: parseIntegerFlag(args, "--lookahead-days", 1),
    perPage: parseIntegerFlag(args, "--per-page", 1),
    outsideGroupLimit: parseIntegerFlag(args, "--top-groups", 1),
    timeoutMs: parseIntegerFlag(args, "--timeout-ms", 1),
    requestIntervalMs: parseIntegerFlag(args, "--request-interval-ms", 0),
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
  const rateLimiter =
    options.requestIntervalMs === undefined || options.requestIntervalMs === 0
      ? undefined
      : createOpenFecRateLimiter({ minIntervalMs: options.requestIntervalMs });

  if (apiKeys.length === 0) {
    throw new Error("No OpenFEC API keys configured. Set FEC_API_KEY_1 or FEC_API_KEY.");
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueCandidateFinance({
      db: pool,
      openFecOptions: { apiKeys, timeoutMs, ...(rateLimiter ? { rateLimiter } : {}) },
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
