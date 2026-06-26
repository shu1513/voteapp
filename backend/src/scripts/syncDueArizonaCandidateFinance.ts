import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isArizonaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueArizonaCandidateFinance,
  type ArizonaCandidateFinanceBatchSyncResult,
} from "../pipeline/arizonaFinance/arizonaCandidateFinanceBatchSync.js";

export type SyncDueArizonaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  timeoutMs?: number;
  directIncomeLimit?: number;
  independentExpenditureLimitPerPosition?: number;
  outsideGroupIncomeLimitPerGroup?: number;
  outsideMaxGroups?: number;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
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

function parseNonNegativeNumberFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

export function parseSyncDueArizonaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueArizonaCandidateFinanceScriptOptions {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    directIncomeLimit: parsePositiveIntegerFlag(args, "--income-limit"),
    independentExpenditureLimitPerPosition: parsePositiveIntegerFlag(args, "--ie-limit"),
    outsideGroupIncomeLimitPerGroup: parsePositiveIntegerFlag(args, "--outside-income-limit"),
    outsideMaxGroups: parsePositiveIntegerFlag(args, "--outside-max-groups"),
    directMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--direct-max-breakdowns"),
    outsideMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--outside-max-breakdowns"),
    minIndustryAmount: parseNonNegativeNumberFlag(args, "--min-industry-amount"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Arizona candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueArizonaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueArizonaCandidateFinanceScriptOptions;
  result: ArizonaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "arizona_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueArizonaCandidateFinanceScriptArgs(process.argv.slice(2));
  if (!isArizonaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Arizona campaign finance due sync disabled; no Arizona data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const result = await syncDueArizonaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      spotlightClientOptions: options.timeoutMs ? { timeoutMs: options.timeoutMs } : undefined,
      directIncomeLimit: options.directIncomeLimit,
      independentExpenditureLimitPerPosition: options.independentExpenditureLimitPerPosition,
      outsideGroupIncomeLimitPerGroup: options.outsideGroupIncomeLimitPerGroup,
      outsideMaxGroups: options.outsideMaxGroups,
      directMaxBreakdownsPerCategory: options.directMaxBreakdownsPerCategory,
      outsideMaxBreakdownsPerCategory: options.outsideMaxBreakdownsPerCategory,
      minIndustryAmount: options.minIndustryAmount,
    });
    console.log(JSON.stringify(toSyncDueArizonaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Arizona candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
