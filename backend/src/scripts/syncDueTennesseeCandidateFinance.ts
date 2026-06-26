import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { loadProjectEnv } from "../config/env.js";
import { isTennesseeCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueTennesseeCandidateFinance,
  type TennesseeCandidateFinanceBatchSyncResult,
} from "../pipeline/tennesseeFinance/tennesseeCandidateFinanceBatchSync.js";
import {
  assertNoUnknownTennesseeFinanceFlags,
  parseTennesseeFinancePositiveIntegerFlag,
} from "./tennesseeCandidateFinanceCliArgs.js";

export type SyncDueTennesseeCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  aiClassifyIndustries: boolean;
  aiClassificationMinAmount?: number;
};

export function parseSyncDueTennesseeCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueTennesseeCandidateFinanceScriptOptions {
  assertNoUnknownTennesseeFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseTennesseeFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseTennesseeFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseTennesseeFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseTennesseeFinancePositiveIntegerFlag(args, "--lookahead-days"),
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
    aiClassificationMinAmount: parseTennesseeFinancePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Tennessee candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueTennesseeCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueTennesseeCandidateFinanceScriptOptions;
  result: TennesseeCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "tennessee_candidate_finance_due_sync",
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
  const options = parseSyncDueTennesseeCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isTennesseeCampaignFinanceSyncEnabled(options.force)) {
    console.log("Tennessee campaign finance due sync disabled; no Tennessee data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueTennesseeCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      financeIndustryClassifier:
        options.aiClassifyIndustries && !options.dryRun ? createFinanceIndustryClassifierFromEnv() : undefined,
      aiClassificationMinAmount: options.aiClassificationMinAmount,
    });

    console.log(JSON.stringify(toSyncDueTennesseeCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Tennessee candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
