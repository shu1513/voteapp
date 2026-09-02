import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isWestVirginiaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { autoLinkMissingWestVirginiaCandidateFinanceLinks } from "../pipeline/westVirginiaFinance/westVirginiaCandidateFinanceAutoLink.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type AutoLinkWestVirginiaCandidateFinanceScriptOptions = {
  force: boolean;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
};

const BOOLEAN_FLAGS = new Set(["--force"]);
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

export function parseAutoLinkWestVirginiaCandidateFinanceScriptArgs(
  args: readonly string[]
): AutoLinkWestVirginiaCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "West Virginia candidate finance auto-link", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    maxCandidates: parsePositiveInteger(args, "--max-candidates", 25),
    electionLookbackDays: parsePositiveInteger(args, "--lookback-days", 78),
    electionLookaheadDays: parsePositiveInteger(args, "--lookahead-days", 730),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for West Virginia candidate finance auto-link");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseAutoLinkWestVirginiaCandidateFinanceScriptArgs(process.argv.slice(2));

  // Auto-link fetches the live committee registry, so it shares the sync gate.
  if (!isWestVirginiaCampaignFinanceSyncEnabled(options.force)) {
    console.log("West Virginia campaign finance sync disabled; no links created");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const results = await autoLinkMissingWestVirginiaCandidateFinanceLinks({
      db: pool,
      now: startedAt,
      maxCandidates: options.maxCandidates,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });
    console.log(
      JSON.stringify(
        {
          type: "west_virginia_candidate_finance_auto_link",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          attempted: results.length,
          linked: results.filter((result) => result.status === "linked").length,
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
      "West Virginia candidate finance auto-link failed:",
      error instanceof Error ? error.message : String(error)
    );
    process.exitCode = 1;
  });
}
