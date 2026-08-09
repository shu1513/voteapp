import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { getPipelineEnv, loadProjectEnv } from "../config/env.js";
import { isSanFranciscoCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueSanFranciscoCandidateFinance,
  type SanFranciscoCandidateFinanceBatchSyncResult,
} from "../pipeline/sanFranciscoFinance/sanFranciscoCandidateFinanceBatchSync.js";

export type SyncDueSanFranciscoCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  /** Historical-backfill targeting; see the batch module's electionId doc. */
  electionId?: string;
};

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set([
  "--max-candidates",
  "--stale-after-days",
  "--lookback-days",
  "--lookahead-days",
  "--election-id",
]);
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function parsePositiveIntegerFlag(
  args: readonly string[],
  flag: string,
): number | undefined {
  const index = args.indexOf(flag);
  if (index === -1) {
    return undefined;
  }
  const raw = args[index + 1];
  const parsed = Number(raw);
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} requires a positive integer, got: ${raw}`);
  }
  return parsed;
}

function parseElectionIdFlag(args: readonly string[]): string | undefined {
  const index = args.indexOf("--election-id");
  if (index === -1) {
    return undefined;
  }
  const raw = args[index + 1];
  if (!raw || !UUID_PATTERN.test(raw)) {
    throw new Error(`--election-id requires an election UUID, got: ${raw}`);
  }
  return raw;
}

export function parseSyncDueSanFranciscoCandidateFinanceScriptArgs(
  args: readonly string[],
): SyncDueSanFranciscoCandidateFinanceScriptOptions {
  // Strict like the Ohio/Georgia sync-due CLIs: a typo (--dryrun) or bare
  // positional ("dry-run" after npm's own "--" separator) must fail loudly
  // instead of silently running a REAL sync.
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (KNOWN_BOOLEAN_FLAGS.has(arg)) {
      continue;
    }
    if (KNOWN_VALUE_FLAGS.has(arg)) {
      index += 1;
      continue;
    }
    throw new Error(`Unknown San Francisco candidate finance flag: ${arg}`);
  }
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    electionId: parseElectionIdFlag(args),
  };
}

export function toSyncDueSanFranciscoCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueSanFranciscoCandidateFinanceScriptOptions;
  result: SanFranciscoCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "san_francisco_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    election_id: input.options.electionId ?? null,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  // Load .env before the flag check; without this the flag reads an unloaded
  // environment and the script silently exits disabled on local runs.
  loadProjectEnv();
  const options = parseSyncDueSanFranciscoCandidateFinanceScriptArgs(
    process.argv.slice(2),
  );

  if (!isSanFranciscoCampaignFinanceSyncEnabled(options.force)) {
    console.log(JSON.stringify({ enabled: false }));
    return;
  }

  const pool = new Pool({ connectionString: getPipelineEnv().DATABASE_URL });
  try {
    const result = await syncDueSanFranciscoCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      electionId: options.electionId,
    });
    console.log(
      JSON.stringify(
        toSyncDueSanFranciscoCandidateFinanceScriptOutput({
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
    console.error(
      "San Francisco candidate finance due sync failed:",
      message,
    );
    process.exitCode = 1;
  });
}
