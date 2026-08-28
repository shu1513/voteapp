import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { syncDueDelawareCandidateFinance } from "../pipeline/delawareFinance/delawareCandidateFinanceBatchSync.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

const SCRIPT_LABEL = "delaware-candidates:finance:sync-due";

function usage(): string {
  return [
    "Usage:",
    "  npm run delaware-candidates:finance:sync-due -- [--max N] [--stale-days N] [--cache-dir DIR] [--no-auto-link] [--write]",
    "",
    "Syncs due Delaware candidates from the CFRS artifact cache (cache-only;",
    "artifact acquisition is a separate Phase 2 step). Dry-run is the",
    "default; --write replaces snapshots in a local database.",
  ].join("\n");
}

export function parseDelawareSyncDueArgs(argv: readonly string[]): {
  maxCandidates: number;
  staleAfterDays: number | undefined;
  cacheDir: string | undefined;
  autoLinkMissingLinks: boolean;
  write: boolean;
} {
  assertKnownCliFlags(SCRIPT_LABEL, argv, [
    { name: "--max", value: "space" },
    { name: "--stale-days", value: "space" },
    { name: "--cache-dir", value: "space" },
    { name: "--no-auto-link", value: "none" },
    { name: "--write", value: "none" },
  ]);
  let maxCandidates = 10;
  let staleAfterDays: number | undefined;
  let cacheDir: string | undefined;
  let autoLinkMissingLinks = true;
  let write = false;
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--write") {
      write = true;
      continue;
    }
    if (token === "--no-auto-link") {
      autoLinkMissingLinks = false;
      continue;
    }
    if (token === "--max" || token === "--stale-days") {
      const value = Number.parseInt(argv[index + 1] ?? "", 10);
      if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error(`Invalid value for ${token}.\n${usage()}`);
      }
      if (token === "--max") maxCandidates = value;
      else staleAfterDays = value;
      index += 1;
      continue;
    }
    if (token === "--cache-dir") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        throw new Error(`Missing value for --cache-dir.\n${usage()}`);
      }
      cacheDir = value;
      index += 1;
    }
  }
  return { maxCandidates, staleAfterDays, cacheDir, autoLinkMissingLinks, write };
}

export async function runSyncDueDelawareCandidateFinance(argv: readonly string[]): Promise<void> {
  const args = parseDelawareSyncDueArgs(argv);
  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Delaware candidate-finance sync");
  }
  if (args.write) {
    requireLocalDatabaseTarget(databaseUrl);
  }
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const result = await syncDueDelawareCandidateFinance({
      db: pool,
      maxCandidates: args.maxCandidates,
      staleAfterDays: args.staleAfterDays,
      cacheDir: args.cacheDir,
      autoLinkMissingLinks: args.autoLinkMissingLinks,
      dryRun: !args.write,
    });
    console.log(JSON.stringify(result, null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  runSyncDueDelawareCandidateFinance(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
