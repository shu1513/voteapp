import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isMontanaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  autoLinkMissingMontanaCandidateFinanceLinks,
  listMontanaCandidateElectionsMissingFinanceLinks,
} from "../pipeline/montanaFinance/montanaCandidateFinanceAutoLink.js";
import {
  assertNoUnknownMontanaFinanceFlags,
  parseMontanaFinancePositiveIntegerFlag,
} from "./montanaCandidateFinanceCliArgs.js";

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_ELECTION_LOOKBACK_DAYS = 55;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

export type AutoLinkMontanaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
};

export function parseAutoLinkMontanaCandidateFinanceScriptArgs(
  args: readonly string[]
): AutoLinkMontanaCandidateFinanceScriptOptions {
  assertNoUnknownMontanaFinanceFlags(args, {
    booleanFlags: ["--dry-run", "--force"],
    valueFlags: ["--max-candidates", "--lookback-days", "--lookahead-days"],
  });
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseMontanaFinancePositiveIntegerFlag(args, "--max-candidates") ?? DEFAULT_MAX_CANDIDATES,
    electionLookbackDays:
      parseMontanaFinancePositiveIntegerFlag(args, "--lookback-days") ?? DEFAULT_ELECTION_LOOKBACK_DAYS,
    electionLookaheadDays:
      parseMontanaFinancePositiveIntegerFlag(args, "--lookahead-days") ?? DEFAULT_ELECTION_LOOKAHEAD_DAYS,
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Montana candidate finance auto-link");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseAutoLinkMontanaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isMontanaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Montana campaign finance sync disabled; no auto-link performed");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    if (options.dryRun) {
      const candidates = await listMontanaCandidateElectionsMissingFinanceLinks(pool, {
        now: startedAt,
        maxCandidates: options.maxCandidates,
        electionLookbackDays: options.electionLookbackDays,
        electionLookaheadDays: options.electionLookaheadDays,
      });
      console.log(
        JSON.stringify(
          {
            type: "montana_candidate_finance_auto_link",
            ts: new Date().toISOString(),
            dry_run: true,
            missing_link_candidate_elections: candidates,
          },
          null,
          2
        )
      );
      return;
    }

    const results = await autoLinkMissingMontanaCandidateFinanceLinks({
      db: pool,
      now: startedAt,
      maxCandidates: options.maxCandidates,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });
    const linked = results.filter((result) => result.status === "linked").length;
    console.log(
      JSON.stringify(
        {
          type: "montana_candidate_finance_auto_link",
          ts: new Date().toISOString(),
          dry_run: false,
          attempted: results.length,
          linked,
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
    const message = error instanceof Error ? error.message : String(error);
    console.error("Montana candidate finance auto-link failed:", message);
    process.exitCode = 1;
  });
}
