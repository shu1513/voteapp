import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isArkansasCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueArkansasCandidateFinance,
  type ArkansasCandidateFinanceBatchSyncResult,
} from "../pipeline/arkansasFinance/arkansasCandidateFinanceBatchSync.js";
import { buildArkansasCfisDnsFallbackFetch } from "../pipeline/arkansasFinance/arkansasCfisDnsFallback.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type SyncDueArkansasCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  dnsFallback: boolean;
  autoLinkMissingLinks: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
};

const BOOLEAN_FLAGS = new Set(["--dry-run", "--force", "--dns-fallback", "--no-auto-link"]);
const VALUE_FLAGS = new Set(["--max-candidates", "--stale-after-days", "--lookback-days", "--lookahead-days"]);

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

export function parseSyncDueArkansasCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueArkansasCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Arkansas candidate finance due sync", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    dnsFallback: args.includes("--dns-fallback"),
    autoLinkMissingLinks: !args.includes("--no-auto-link"),
    maxCandidates: parsePositiveInteger(args, "--max-candidates"),
    staleAfterDays: parsePositiveInteger(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveInteger(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveInteger(args, "--lookahead-days"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Arkansas candidate finance due sync");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueArkansasCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isArkansasCampaignFinanceSyncEnabled(options.force)) {
    console.log("Arkansas campaign finance due sync disabled; no Arkansas data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const result: ArkansasCandidateFinanceBatchSyncResult = await syncDueArkansasCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      autoLinkMissingLinks: options.autoLinkMissingLinks,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      ...(options.dnsFallback ? { clientOptions: { fetchImpl: await buildArkansasCfisDnsFallbackFetch() } } : {}),
    });
    console.log(
      JSON.stringify(
        {
          type: "arkansas_candidate_finance_due_sync",
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
      "Arkansas candidate finance due sync failed:",
      error instanceof Error ? error.message : String(error)
    );
    process.exitCode = 1;
  });
}
