import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isNewHampshireCfsRawDataRefreshEnabled } from "../config/featureFlags.js";
import { autoLinkMissingNewHampshireCandidateFinanceLinks } from "../pipeline/newHampshireFinance/newHampshireCandidateFinanceAutoLink.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";
import { parseNewHampshireFinancePositiveIntegerFlag } from "./newHampshireCandidateFinanceCliArgs.js";

export type AutoLinkNewHampshireCandidateFinanceScriptOptions = {
  force: boolean;
  dryRun: boolean;
  /** null = every due candidate (see listNewHampshireCandidateElectionsMissingFinanceLinks). */
  maxCandidates: number | null;
  electionLookbackDays: number;
  electionLookaheadDays: number;
};

const BOOLEAN_FLAGS = new Set(["--force", "--dry-run"]);
const VALUE_FLAGS = new Set(["--max-candidates", "--lookback-days", "--lookahead-days"]);

export function parseAutoLinkNewHampshireCandidateFinanceScriptArgs(
  args: readonly string[]
): AutoLinkNewHampshireCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "New Hampshire candidate finance auto-link", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    dryRun: args.includes("--dry-run"),
    maxCandidates: parseNewHampshireFinancePositiveIntegerFlag(args, "--max-candidates", null),
    electionLookbackDays: parseNewHampshireFinancePositiveIntegerFlag(args, "--lookback-days", 30),
    electionLookaheadDays: parseNewHampshireFinancePositiveIntegerFlag(args, "--lookahead-days", 730),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for New Hampshire candidate finance auto-link");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseAutoLinkNewHampshireCandidateFinanceScriptArgs(process.argv.slice(2));

  // Auto-link hits the live CFS API, so it shares the live-call gate (the
  // master flag plus the raw-refresh flag, or --force; North Dakota precedent).
  if (!isNewHampshireCfsRawDataRefreshEnabled(options.force)) {
    console.log("New Hampshire campaign finance auto-link disabled; no links created");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const results = await autoLinkMissingNewHampshireCandidateFinanceLinks({
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
          type: "new_hampshire_candidate_finance_auto_link",
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
    console.error(
      "New Hampshire candidate finance auto-link failed:",
      error instanceof Error ? error.message : String(error)
    );
    process.exitCode = 1;
  });
}
