import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isSouthCarolinaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueSouthCarolinaCandidateFinance,
  type SouthCarolinaCandidateFinanceBatchSyncResult,
} from "../pipeline/southCarolinaFinance/southCarolinaCandidateFinanceBatchSync.js";
import {
  assertNoUnknownSouthCarolinaFinanceFlags,
  parseSouthCarolinaFinancePositiveIntegerFlag,
} from "./southCarolinaCandidateFinanceCliArgs.js";

export type SyncDueSouthCarolinaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  autoLinkMissingLinks: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
};

export function parseSyncDueSouthCarolinaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueSouthCarolinaCandidateFinanceScriptOptions {
  assertNoUnknownSouthCarolinaFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    autoLinkMissingLinks: !args.includes("--no-auto-link"),
    maxCandidates: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--lookahead-days"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for South Carolina candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueSouthCarolinaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueSouthCarolinaCandidateFinanceScriptOptions;
  result: SouthCarolinaCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "south_carolina_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueSouthCarolinaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isSouthCarolinaCampaignFinanceSyncEnabled(options.force)) {
    console.log("South Carolina campaign finance due sync disabled; no South Carolina data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueSouthCarolinaCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      autoLinkMissingLinks: options.autoLinkMissingLinks,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });

    console.log(
      JSON.stringify(toSyncDueSouthCarolinaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2)
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("South Carolina candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
