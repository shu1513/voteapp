import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isSouthCarolinaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  autoLinkMissingSouthCarolinaCandidateFinanceLinks,
  listSouthCarolinaCandidateElectionsMissingFinanceLinks,
} from "../pipeline/southCarolinaFinance/southCarolinaCandidateFinanceAutoLink.js";
import {
  assertNoUnknownSouthCarolinaFinanceFlags,
  parseSouthCarolinaFinancePositiveIntegerFlag,
} from "./southCarolinaCandidateFinanceCliArgs.js";

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_ELECTION_LOOKBACK_DAYS = 76;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

export type AutoLinkSouthCarolinaCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
};

export function parseAutoLinkSouthCarolinaCandidateFinanceScriptArgs(
  args: readonly string[]
): AutoLinkSouthCarolinaCandidateFinanceScriptOptions {
  assertNoUnknownSouthCarolinaFinanceFlags(args);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--max-candidates") ?? DEFAULT_MAX_CANDIDATES,
    electionLookbackDays:
      parseSouthCarolinaFinancePositiveIntegerFlag(args, "--lookback-days") ?? DEFAULT_ELECTION_LOOKBACK_DAYS,
    electionLookaheadDays:
      parseSouthCarolinaFinancePositiveIntegerFlag(args, "--lookahead-days") ?? DEFAULT_ELECTION_LOOKAHEAD_DAYS,
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for South Carolina candidate finance auto-link");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseAutoLinkSouthCarolinaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isSouthCarolinaCampaignFinanceSyncEnabled(options.force)) {
    console.log("South Carolina campaign finance sync disabled; no auto-link performed");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    if (options.dryRun) {
      const candidates = await listSouthCarolinaCandidateElectionsMissingFinanceLinks(pool, {
        now: startedAt,
        maxCandidates: options.maxCandidates,
        electionLookbackDays: options.electionLookbackDays,
        electionLookaheadDays: options.electionLookaheadDays,
      });
      console.log(
        JSON.stringify(
          {
            type: "south_carolina_candidate_finance_auto_link",
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

    const results = await autoLinkMissingSouthCarolinaCandidateFinanceLinks({
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
          type: "south_carolina_candidate_finance_auto_link",
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
    console.error("South Carolina candidate finance auto-link failed:", message);
    process.exitCode = 1;
  });
}
