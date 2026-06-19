import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { loadProjectEnv } from "../config/env.js";
import { isCaliforniaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueCaliforniaCandidateFinance,
  type CaliforniaCandidateFinanceBatchSyncResult,
} from "../pipeline/californiaFinance/californiaCandidateFinanceBatchSync.js";

export type SyncDueCaliforniaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  includeOutside: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  timeoutMs?: number;
  rawZipPath?: string;
  rawCacheDir?: string;
  aiClassifyIndustries: boolean;
  aiClassificationMinAmount?: number;
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

export function parseSyncDueCaliforniaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueCaliforniaCandidateFinanceScriptOptions {
  return {
    dryRun: args.includes("--dry-run"),
    includeOutside: !args.includes("--skip-outside"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    rawZipPath: parseFlagValue(args, "--raw-zip")?.trim() || undefined,
    rawCacheDir: parseFlagValue(args, "--raw-cache-dir")?.trim() || undefined,
    aiClassifyIndustries: args.includes("--ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

function getDatabaseUrl(): string {
  return process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp";
}

export function toSyncDueCaliforniaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueCaliforniaCandidateFinanceScriptOptions;
  result: CaliforniaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "california_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    include_outside: input.options.includeOutside,
    ai_classify_industries: input.options.aiClassifyIndustries,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueCaliforniaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isCaliforniaCampaignFinanceSyncEnabled(options.force)) {
    console.log("California campaign finance due sync disabled; no California data fetched");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueCaliforniaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      includeOutside: options.includeOutside,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      powerSearchOptions: options.timeoutMs ? { timeoutMs: options.timeoutMs } : undefined,
      rawDataZipPath: options.rawZipPath,
      rawDataCacheDir: options.rawCacheDir,
      financeIndustryClassifier:
        options.aiClassifyIndustries && !options.dryRun ? createFinanceIndustryClassifierFromEnv() : undefined,
      aiClassificationMinAmount: options.aiClassificationMinAmount,
    });

    console.log(JSON.stringify(toSyncDueCaliforniaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("California candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
