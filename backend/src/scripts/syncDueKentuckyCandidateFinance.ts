import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isKentuckyCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueKentuckyCandidateFinance,
  type KentuckyCandidateFinanceBatchSyncResult,
} from "../pipeline/kentuckyFinance/kentuckyCandidateFinanceBatchSync.js";

export type SyncDueKentuckyCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
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

function assertNoUnknownFlags(args: readonly string[], supportedFlags: ReadonlySet<string>): void {
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!supportedFlags.has(name)) {
      throw new Error(`Unknown Kentucky candidate finance due sync flag: ${name}`);
    }
  }
}

export function parseSyncDueKentuckyCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueKentuckyCandidateFinanceScriptOptions {
  assertNoUnknownFlags(
    args,
    new Set(["--dry-run", "--force", "--max-candidates", "--stale-after-days", "--lookback-days", "--lookahead-days"])
  );
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Kentucky candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueKentuckyCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueKentuckyCandidateFinanceScriptOptions;
  result: KentuckyCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "kentucky_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueKentuckyCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isKentuckyCampaignFinanceSyncEnabled(options.force)) {
    console.log("Kentucky campaign finance due sync disabled; no Kentucky data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueKentuckyCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      autoLinkMissingLinks: false,
    });

    console.log(JSON.stringify(toSyncDueKentuckyCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Kentucky candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
