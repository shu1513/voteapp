import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isAlabamaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueAlabamaCandidateFinance,
  type AlabamaCandidateFinanceBatchSyncResult,
} from "../pipeline/alabamaFinance/alabamaCandidateFinanceBatchSync.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type SyncDueAlabamaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  autoLinkMissingLinks: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  cacheDir?: string;
};

const BOOLEAN_FLAGS = new Set(["--dry-run", "--force", "--no-auto-link"]);
const VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--cache-dir",
]);

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const values: string[] = [];
  const prefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg.startsWith(prefix)) {
      values.push(arg.slice(prefix.length).trim());
    } else if (arg === name) {
      values.push(args[index + 1]!.trim());
      index += 1;
    }
  }
  if (values.length > 1) throw new Error(`Provide ${name} at most once`);
  return values[0];
}

function parsePositiveInteger(args: readonly string[], name: string): number | undefined {
  const value = readValueFlag(args, name);
  if (value === undefined) return undefined;
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value))) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

export function parseSyncDueAlabamaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueAlabamaCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Alabama candidate finance due sync", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    autoLinkMissingLinks: !args.includes("--no-auto-link"),
    maxCandidates: parsePositiveInteger(args, "--max-candidates"),
    staleAfterDays: parsePositiveInteger(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveInteger(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveInteger(args, "--lookahead-days"),
    cacheDir: readValueFlag(args, "--cache-dir"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Alabama candidate finance due sync");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueAlabamaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isAlabamaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Alabama campaign finance due sync disabled; no Alabama data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const result: AlabamaCandidateFinanceBatchSyncResult = await syncDueAlabamaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      autoLinkMissingLinks: options.autoLinkMissingLinks,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      cacheDir: options.cacheDir,
    });
    console.log(
      JSON.stringify(
        {
          type: "alabama_candidate_finance_due_sync",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          dry_run: options.dryRun,
          result,
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error(
      "Alabama candidate finance due sync failed:",
      error instanceof Error ? error.message : String(error)
    );
    process.exitCode = 1;
  });
}
