import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isIdahoCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { autoLinkMissingIdahoCandidateFinanceLinks } from "../pipeline/idahoFinance/idahoCandidateFinanceAutoLink.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";
import { parseIdahoFinancePositiveIntegerFlag } from "./idahoCandidateFinanceCliArgs.js";

export type AutoLinkIdahoCandidateFinanceScriptOptions = {
  force: boolean;
  dryRun: boolean;
  /** null = every due candidate (see listIdahoCandidateElectionsMissingFinanceLinks). */
  maxCandidates: number | null;
  electionLookbackDays: number;
  electionLookaheadDays: number;
};

const BOOLEAN_FLAGS = new Set(["--force", "--dry-run"]);
const VALUE_FLAGS = new Set(["--max-candidates", "--lookback-days", "--lookahead-days"]);

export function parseAutoLinkIdahoCandidateFinanceScriptArgs(
  args: readonly string[]
): AutoLinkIdahoCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Idaho candidate finance auto-link", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    dryRun: args.includes("--dry-run"),
    maxCandidates: parseIdahoFinancePositiveIntegerFlag(args, "--max-candidates", null),
    electionLookbackDays: parseIdahoFinancePositiveIntegerFlag(args, "--lookback-days", 98),
    electionLookaheadDays: parseIdahoFinancePositiveIntegerFlag(args, "--lookahead-days", 730),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Idaho candidate finance auto-link");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseAutoLinkIdahoCandidateFinanceScriptArgs(process.argv.slice(2));

  // Auto-link hits the live Sunshine API, so it shares the sync gate.
  if (!isIdahoCampaignFinanceSyncEnabled(options.force)) {
    console.log("Idaho campaign finance sync disabled; no links created");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const results = await autoLinkMissingIdahoCandidateFinanceLinks({
      db: pool,
      now: startedAt,
      maxCandidates: options.maxCandidates,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      dryRun: options.dryRun,
    });
    const count = (status: string) => results.filter((result) => result.status === status).length;
    console.log(
      JSON.stringify(
        {
          type: "idaho_candidate_finance_auto_link",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          dry_run: options.dryRun,
          attempted: results.length,
          linked: count("linked"),
          ambiguous: count("ambiguous"),
          unmatched: count("unmatched"),
          errors: count("error"),
          results,
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
    console.error("Idaho candidate finance auto-link failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
