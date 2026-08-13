// Denver candidate finance due sync (plan-denver-finance.md Phase 3).
// Runs the auto-link leg plus the due-list sync loop against SearchLight;
// gated by DENVER_CAMPAIGN_FINANCE_ENABLED + DENVER_CAMPAIGN_FINANCE_SYNC_ENABLED
// (or --force). --dry-run performs every fetch and reconciliation but writes
// nothing (the auto-link leg is skipped entirely — it writes link rows).

import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isDenverCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueDenverCandidateFinance,
  type DenverCandidateFinanceBatchSyncResult,
} from "../pipeline/denverFinance/denverCandidateFinanceBatchSync.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type SyncDueDenverCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  autoLink: boolean;
  bypassAnomalyCheck: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
};

const KNOWN_BOOLEAN_FLAGS = new Set([
  "--bypass-anomaly-check",
  "--dry-run",
  "--force",
  "--no-auto-link",
]);
const KNOWN_VALUE_FLAGS = new Set([
  "--lookahead-days",
  "--lookback-days",
  "--max-candidates",
  "--stale-after-days",
]);

function parsePositiveIntegerFlag(
  args: readonly string[],
  name: string,
): number | undefined {
  const values: string[] = [];
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg.startsWith(`${name}=`)) {
      values.push(arg.slice(name.length + 1).trim());
      continue;
    }
    if (arg === name) {
      values.push((args[index + 1] ?? "").trim());
      index += 1;
    }
  }
  if (values.length === 0) return undefined;
  if (values.length > 1) throw new Error(`Provide ${name} at most once`);
  const raw = values[0]!;
  if (!/^[1-9]\d*$/.test(raw)) throw new Error(`Invalid ${name} value: ${raw}`);
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed))
    throw new Error(`Invalid ${name} value: ${raw}`);
  return parsed;
}

export function parseSyncDueDenverCandidateFinanceScriptArgs(
  args: readonly string[],
): SyncDueDenverCandidateFinanceScriptOptions {
  assertKnownCliFlags(
    args,
    "Denver candidate finance due sync",
    KNOWN_BOOLEAN_FLAGS,
    KNOWN_VALUE_FLAGS,
  );
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    autoLink: !args.includes("--no-auto-link"),
    bypassAnomalyCheck: args.includes("--bypass-anomaly-check"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl)
    throw new Error(
      "DATABASE_URL is required for Denver candidate finance due sync",
    );
  return databaseUrl;
}

export function toSyncDueDenverCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueDenverCandidateFinanceScriptOptions;
  result: DenverCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "denver_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueDenverCandidateFinanceScriptArgs(
    process.argv.slice(2),
  );

  if (!isDenverCampaignFinanceSyncEnabled(options.force)) {
    console.log("Denver campaign finance due sync disabled; nothing synced");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const result = await syncDueDenverCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      autoLink: options.autoLink,
      bypassAnomalyCheck: options.bypassAnomalyCheck,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });
    console.log(
      JSON.stringify(
        toSyncDueDenverCandidateFinanceScriptOutput({
          startedAt,
          options,
          result,
        }),
        null,
        2,
      ),
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Denver candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
