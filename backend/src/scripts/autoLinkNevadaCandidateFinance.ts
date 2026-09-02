import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  autoLinkNevadaCandidateFinance,
} from "../pipeline/nevadaFinance/nevadaCandidateFinanceImport.js";
import { DEFAULT_NEVADA_AURORA_ARTIFACT_DIR } from "../pipeline/nevadaFinance/nevadaAuroraArtifacts.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

const SCRIPT_LABEL = "nevada-candidates:finance:auto-link";

function usage(): string {
  return [
    "Usage:",
    "  npm run nevada-candidates:finance:auto-link -- [--artifact-dir DIR] [--election-date 2026-11-03] [--write]",
    "",
    "Matches VoteApp Nevada candidates to harvested AURORA roster artifacts.",
    "Dry-run is the default; --write upserts links into a local database.",
  ].join("\n");
}

export function parseNevadaAutoLinkArgs(argv: readonly string[]): {
  artifactDir: string;
  electionDate: string;
  write: boolean;
} {
  assertKnownCliFlags(SCRIPT_LABEL, argv, [
    { name: "--artifact-dir", value: "space" },
    { name: "--election-date", value: "space" },
    { name: "--write", value: "none" },
  ]);
  let artifactDir = DEFAULT_NEVADA_AURORA_ARTIFACT_DIR;
  let electionDate = "2026-11-03";
  let write = false;
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--write") {
      write = true;
      continue;
    }
    if (token === "--artifact-dir" || token === "--election-date") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        throw new Error(`Missing value for ${token}.\n${usage()}`);
      }
      if (token === "--artifact-dir") artifactDir = value;
      else electionDate = value;
      index += 1;
    }
  }
  return { artifactDir, electionDate, write };
}

export async function runAutoLinkNevadaCandidateFinance(argv: readonly string[]): Promise<void> {
  const args = parseNevadaAutoLinkArgs(argv);
  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Nevada candidate-finance auto-link");
  }
  if (args.write) {
    requireLocalDatabaseTarget(databaseUrl);
  }
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const result = await autoLinkNevadaCandidateFinance({
      db: pool,
      artifactDir: args.artifactDir,
      electionDate: args.electionDate,
      write: args.write,
    });
    console.log(JSON.stringify({ dryRun: !args.write, ...result }, null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  runAutoLinkNevadaCandidateFinance(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
