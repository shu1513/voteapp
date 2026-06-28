import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { loadProjectEnv } from "../config/env.js";
import { isFloridaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncFloridaCandidateFinanceBatch,
  type FloridaCandidateFinanceBatchSyncResult,
} from "../pipeline/floridaFinance/floridaCandidateFinanceBatchSync.js";

export type SyncDueFloridaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  defaultArtifactCacheDir?: string;
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

export function parseSyncDueFloridaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueFloridaCandidateFinanceScriptOptions {
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    defaultArtifactCacheDir: parseFlagValue(args, "--artifact-cache-dir") || undefined,
    aiClassifyIndustries: args.includes("--ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-classification-min-amount"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Florida candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueFloridaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueFloridaCandidateFinanceScriptOptions;
  result: FloridaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "florida_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueFloridaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isFloridaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Florida campaign finance due sync disabled; no Florida data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncFloridaCandidateFinanceBatch({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      defaultArtifactCacheDir: options.defaultArtifactCacheDir,
      financeIndustryClassifier:
        options.aiClassifyIndustries && !options.dryRun ? createFinanceIndustryClassifierFromEnv() : undefined,
      aiClassificationMinAmount: options.aiClassificationMinAmount,
    });

    console.log(JSON.stringify(toSyncDueFloridaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Florida candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
