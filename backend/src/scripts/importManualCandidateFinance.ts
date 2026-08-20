import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  persistManualCandidateFinanceFilings,
  planManualCandidateFinanceImport,
} from "../pipeline/finance/manualCandidateFinancePersistence.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { validateManualCandidateFinanceFile } from "./validateManualCandidateFinance.js";

const SCRIPT_LABEL = "manual:candidate-finance:import";

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-finance:import -- --file filing-a.json [--file filing-b.json ...] [--write]",
    "",
    "Validates filing lineage and VoteApp candidate/election links, then prints an import plan.",
    "Dry-run is the default. --write is explicit and restricted to a local PostgreSQL target.",
  ].join("\n");
}

export function parseManualCandidateFinanceImportArgs(argv: readonly string[]): {
  files: string[];
  write: boolean;
} {
  assertKnownCliFlags(SCRIPT_LABEL, argv, [
    { name: "--file", value: "space" },
    { name: "--write", value: "none" },
  ]);

  const files: string[] = [];
  let writeCount = 0;
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--write") {
      writeCount += 1;
      continue;
    }
    if (token !== "--file") {
      continue;
    }
    const value = argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for --file.\n${usage()}`);
    }
    files.push(value.trim());
    index += 1;
  }
  if (files.length === 0) {
    throw new Error(`At least one --file is required.\n${usage()}`);
  }
  if (writeCount > 1) {
    throw new Error(`--write may be provided only once.\n${usage()}`);
  }
  return { files, write: writeCount === 1 };
}

function requireDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for manual candidate-finance import");
  }
  return databaseUrl;
}

export async function runImportManualCandidateFinance(argv: readonly string[]): Promise<void> {
  const args = parseManualCandidateFinanceImportArgs(argv);
  const payloads = await Promise.all(args.files.map((file) => validateManualCandidateFinanceFile(file)));

  loadProjectEnv();
  const databaseUrl = requireDatabaseUrl();
  if (args.write) {
    requireLocalDatabaseTarget(databaseUrl);
  }

  const pool = new Pool({ connectionString: databaseUrl });
  try {
    if (args.write) {
      const result = await persistManualCandidateFinanceFilings({ db: pool, payloads });
      console.log(JSON.stringify({ dryRun: false, files: args.files, result }, null, 2));
    } else {
      const plan = await planManualCandidateFinanceImport({ db: pool, payloads });
      console.log(JSON.stringify({ dryRun: true, files: args.files, plan }, null, 2));
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  runImportManualCandidateFinance(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
