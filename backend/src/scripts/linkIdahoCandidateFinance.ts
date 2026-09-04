import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isIdahoCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { linkIdahoCandidateFinanceManually } from "../pipeline/idahoFinance/idahoCandidateFinanceManualLink.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";
import { parseIdahoFinanceRequiredStringFlag } from "./idahoCandidateFinanceCliArgs.js";

export type LinkIdahoCandidateFinanceScriptOptions = {
  force: boolean;
  dryRun: boolean;
  candidateId: string;
  electionId: string;
  registrationGuid: string;
};

const BOOLEAN_FLAGS = new Set(["--force", "--dry-run"]);
const VALUE_FLAGS = new Set(["--candidate-id", "--election-id", "--registration-guid"]);

export function parseLinkIdahoCandidateFinanceScriptArgs(args: readonly string[]): LinkIdahoCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Idaho candidate finance manual link", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    dryRun: args.includes("--dry-run"),
    candidateId: parseIdahoFinanceRequiredStringFlag(args, "--candidate-id"),
    electionId: parseIdahoFinanceRequiredStringFlag(args, "--election-id"),
    registrationGuid: parseIdahoFinanceRequiredStringFlag(args, "--registration-guid"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Idaho candidate finance manual link");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseLinkIdahoCandidateFinanceScriptArgs(process.argv.slice(2));

  // The link is validated against the live Sunshine grid, so it shares the
  // sync gate with the auto-link.
  if (!isIdahoCampaignFinanceSyncEnabled(options.force)) {
    console.log("Idaho campaign finance sync disabled; no link created");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    const result = await linkIdahoCandidateFinanceManually({
      db: pool,
      candidateId: options.candidateId,
      electionId: options.electionId,
      registrationGuid: options.registrationGuid,
      now: startedAt,
      dryRun: options.dryRun,
    });
    console.log(
      JSON.stringify(
        {
          type: "idaho_candidate_finance_manual_link",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          ...result,
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
    console.error("Idaho candidate finance manual link failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
