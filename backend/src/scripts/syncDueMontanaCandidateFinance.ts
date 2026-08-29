import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isMontanaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueMontanaCandidateFinance,
  type MontanaCandidateFinanceBatchSyncResult,
} from "../pipeline/montanaFinance/montanaCandidateFinanceBatchSync.js";
import {
  assertNoUnknownMontanaFinanceFlags,
  parseMontanaFinancePositiveIntegerFlag,
} from "./montanaCandidateFinanceCliArgs.js";

export type SyncDueMontanaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  autoLinkMissingLinks: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
};

export function parseSyncDueMontanaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueMontanaCandidateFinanceScriptOptions {
  assertNoUnknownMontanaFinanceFlags(args, {
    booleanFlags: ["--dry-run", "--force", "--no-auto-link"],
    valueFlags: ["--max-candidates", "--stale-after-days", "--lookback-days", "--lookahead-days"],
  });
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    autoLinkMissingLinks: !args.includes("--no-auto-link"),
    maxCandidates: parseMontanaFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseMontanaFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseMontanaFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseMontanaFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Montana candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueMontanaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueMontanaCandidateFinanceScriptOptions;
  result: MontanaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "montana_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueMontanaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isMontanaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Montana campaign finance due sync disabled; no Montana data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueMontanaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      forceRawDataRefresh: options.force,
      autoLinkMissingLinks: options.autoLinkMissingLinks,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });

    console.log(
      JSON.stringify(toSyncDueMontanaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2)
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Montana candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
