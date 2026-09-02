import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isKansasCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { autoLinkMissingKansasCandidateFinanceLinks } from "../pipeline/kansasFinance/kansasCandidateFinanceAutoLink.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type AutoLinkKansasCandidateFinanceScriptOptions = {
  force: boolean;
  dryRun: boolean;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
};

const BOOLEAN_FLAGS = new Set(["--force", "--dry-run"]);
const VALUE_FLAGS = new Set(["--max-candidates", "--lookback-days", "--lookahead-days"]);

function parsePositiveInteger(args: readonly string[], name: string, fallback: number): number {
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
  const value = values[0];
  if (value === undefined) return fallback;
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value))) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

export function parseAutoLinkKansasCandidateFinanceScriptArgs(
  args: readonly string[]
): AutoLinkKansasCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Kansas candidate finance auto-link", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    dryRun: args.includes("--dry-run"),
    maxCandidates: parsePositiveInteger(args, "--max-candidates", 25),
    electionLookbackDays: parsePositiveInteger(args, "--lookback-days", 98),
    electionLookaheadDays: parsePositiveInteger(args, "--lookahead-days", 730),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Kansas candidate finance auto-link");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseAutoLinkKansasCandidateFinanceScriptArgs(process.argv.slice(2));

  // Auto-link hits the live SOS CFR viewer, so it shares the sync gate.
  if (!isKansasCampaignFinanceSyncEnabled(options.force)) {
    console.log("Kansas campaign finance sync disabled; no links created");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const results = await autoLinkMissingKansasCandidateFinanceLinks({
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
          type: "kansas_candidate_finance_auto_link",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          dry_run: options.dryRun,
          attempted: results.length,
          linked: count("linked"),
          ambiguous: count("ambiguous"),
          manual_confirm_required: count("manual_confirm_required"),
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
    console.error("Kansas candidate finance auto-link failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
