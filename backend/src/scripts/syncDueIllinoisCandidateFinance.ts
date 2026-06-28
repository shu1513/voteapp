import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { loadProjectEnv } from "../config/env.js";
import { isIllinoisCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueIllinoisCandidateFinance,
  type IllinoisCandidateFinanceBatchSyncResult,
} from "../pipeline/illinoisFinance/illinoisCandidateFinanceBatchSync.js";

export type SyncDueIllinoisCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  timeoutMs?: number;
  aiClassifyIndustries: boolean;
  aiClassificationMinAmount?: number;
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

function assertNoUnknownFlags(args: readonly string[], allowedFlags: readonly string[]): void {
  const allowed = new Set(allowedFlags);
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const flag = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!allowed.has(flag)) {
      throw new Error(`Unknown option: ${flag}`);
    }
  }
}

export function parseSyncDueIllinoisCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueIllinoisCandidateFinanceScriptOptions {
  assertNoUnknownFlags(args, [
    "--dry-run",
    "--force",
    "--max-candidates",
    "--stale-after-days",
    "--lookback-days",
    "--lookahead-days",
    "--timeout-ms",
    "--ai-classify-industries",
    "--no-ai-classify-industries",
    "--ai-min-amount",
  ]);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Illinois candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueIllinoisCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueIllinoisCandidateFinanceScriptOptions;
  result: IllinoisCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "illinois_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    ai_classify_industries: input.options.aiClassifyIndustries,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueIllinoisCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isIllinoisCampaignFinanceSyncEnabled(options.force)) {
    console.log("Illinois campaign finance due sync disabled; no Illinois data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueIllinoisCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      sbeClientOptions: options.timeoutMs ? { timeoutMs: options.timeoutMs } : undefined,
      financeIndustryClassifier:
        options.aiClassifyIndustries && !options.dryRun ? createFinanceIndustryClassifierFromEnv() : undefined,
      aiClassificationMinAmount: options.aiClassificationMinAmount,
    });

    console.log(JSON.stringify(toSyncDueIllinoisCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Illinois candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
