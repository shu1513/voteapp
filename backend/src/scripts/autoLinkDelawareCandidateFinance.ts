import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  autoLinkMissingDelawareCandidateFinanceLinks,
  listDelawareCandidateElectionsMissingFinanceLinks,
} from "../pipeline/delawareFinance/delawareCandidateFinanceAutoLink.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

const SCRIPT_LABEL = "delaware-candidates:finance:auto-link";

function usage(): string {
  return [
    "Usage:",
    "  npm run delaware-candidates:finance:auto-link -- [--max N] [--lookback-days N] [--lookahead-days N] [--write]",
    "",
    "Resolves VoteApp Delaware candidates to CFRS committees via the live",
    "office-filtered committee search. Dry-run (list only) is the default;",
    "--write upserts exact matches into a local database as cfrs_portal links.",
  ].join("\n");
}

export function parseDelawareAutoLinkArgs(argv: readonly string[]): {
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  write: boolean;
} {
  assertKnownCliFlags(SCRIPT_LABEL, argv, [
    { name: "--max", value: "space" },
    { name: "--lookback-days", value: "space" },
    { name: "--lookahead-days", value: "space" },
    { name: "--write", value: "none" },
  ]);
  let maxCandidates = 25;
  let electionLookbackDays = 60;
  let electionLookaheadDays = 730;
  let write = false;
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--write") {
      write = true;
      continue;
    }
    if (token === "--max" || token === "--lookback-days" || token === "--lookahead-days") {
      const value = Number.parseInt(argv[index + 1] ?? "", 10);
      if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error(`Invalid value for ${token}.\n${usage()}`);
      }
      if (token === "--max") maxCandidates = value;
      else if (token === "--lookback-days") electionLookbackDays = value;
      else electionLookaheadDays = value;
      index += 1;
    }
  }
  return { maxCandidates, electionLookbackDays, electionLookaheadDays, write };
}

export async function runAutoLinkDelawareCandidateFinance(argv: readonly string[]): Promise<void> {
  const args = parseDelawareAutoLinkArgs(argv);
  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Delaware candidate-finance auto-link");
  }
  if (args.write) {
    requireLocalDatabaseTarget(databaseUrl);
  }
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const now = new Date();
    if (!args.write) {
      const missing = await listDelawareCandidateElectionsMissingFinanceLinks(pool, {
        now,
        maxCandidates: args.maxCandidates,
        electionLookbackDays: args.electionLookbackDays,
        electionLookaheadDays: args.electionLookaheadDays,
      });
      console.log(JSON.stringify({ dryRun: true, missingLinkCandidates: missing }, null, 2));
      return;
    }
    const results = await autoLinkMissingDelawareCandidateFinanceLinks({
      db: pool,
      now,
      maxCandidates: args.maxCandidates,
      electionLookbackDays: args.electionLookbackDays,
      electionLookaheadDays: args.electionLookaheadDays,
    });
    console.log(JSON.stringify({ dryRun: false, results }, null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  runAutoLinkDelawareCandidateFinance(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
