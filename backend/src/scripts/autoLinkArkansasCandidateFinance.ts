import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isArkansasCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { autoLinkMissingArkansasCandidateFinanceLinks } from "../pipeline/arkansasFinance/arkansasCandidateFinanceAutoLink.js";
import { buildArkansasCfisDnsFallbackFetch } from "../pipeline/arkansasFinance/arkansasCfisDnsFallback.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type AutoLinkArkansasCandidateFinanceScriptOptions = {
  force: boolean;
  dnsFallback: boolean;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
};

const BOOLEAN_FLAGS = new Set(["--force", "--dns-fallback"]);
const VALUE_FLAGS = new Set(["--max-candidates", "--lookback-days", "--lookahead-days"]);

function parsePositiveInteger(
  args: readonly string[],
  name: string,
  fallback: number
): number {
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

export function parseAutoLinkArkansasCandidateFinanceScriptArgs(
  args: readonly string[]
): AutoLinkArkansasCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Arkansas candidate finance auto-link", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    dnsFallback: args.includes("--dns-fallback"),
    maxCandidates: parsePositiveInteger(args, "--max-candidates", 25),
    electionLookbackDays: parsePositiveInteger(args, "--lookback-days", 98),
    electionLookaheadDays: parsePositiveInteger(args, "--lookahead-days", 730),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Arkansas candidate finance auto-link");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseAutoLinkArkansasCandidateFinanceScriptArgs(process.argv.slice(2));

  // Auto-link hits the live CFIS API, so it shares the sync gate.
  if (!isArkansasCampaignFinanceSyncEnabled(options.force)) {
    console.log("Arkansas campaign finance sync disabled; no links created");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const results = await autoLinkMissingArkansasCandidateFinanceLinks({
      db: pool,
      now: startedAt,
      maxCandidates: options.maxCandidates,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      ...(options.dnsFallback
        ? { clientOptions: { fetchImpl: await buildArkansasCfisDnsFallbackFetch() } }
        : {}),
    });
    console.log(
      JSON.stringify(
        {
          type: "arkansas_candidate_finance_auto_link",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          dns_fallback: options.dnsFallback,
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
      "Arkansas candidate finance auto-link failed:",
      error instanceof Error ? error.message : String(error)
    );
    process.exitCode = 1;
  });
}
