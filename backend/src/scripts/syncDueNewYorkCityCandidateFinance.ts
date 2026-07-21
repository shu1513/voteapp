import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isNewYorkCityCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { syncDueNewYorkCityCandidateFinance } from "../pipeline/newYorkCityFinance/newYorkCityCandidateFinanceBatchSync.js";

function flagValue(args: readonly string[], name: string): string | null {
  const inline = args.find((arg) => arg.startsWith(`${name}=`));
  if (inline) return inline.slice(name.length + 1).trim() || null;
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1]?.trim() || null : null;
}
function positiveInteger(args: readonly string[], name: string): number | undefined {
  const raw = flagValue(args, name);
  if (raw === null) return undefined;
  if (!/^[1-9]\d*$/.test(raw)) throw new Error(`Invalid ${name}: ${raw}`);
  return Number(raw);
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = process.argv.slice(2);
  const force = args.includes("--force");
  if (!isNewYorkCityCampaignFinanceSyncEnabled(force)) {
    console.log(JSON.stringify({ type: "new_york_city_candidate_finance_due_sync", enabled: false }));
    return;
  }
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) throw new Error("DATABASE_URL is required for NYC candidate finance sync");
  const dryRun = args.includes("--dry-run");
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const result = await syncDueNewYorkCityCandidateFinance({
      db: pool,
      dryRun,
      maxCandidates: positiveInteger(args, "--max-candidates"),
      staleAfterDays: positiveInteger(args, "--stale-after-days"),
      electionLookbackDays: positiveInteger(args, "--lookback-days"),
      electionLookaheadDays: positiveInteger(args, "--lookahead-days"),
      cacheDir: flagValue(args, "--cache-dir") ?? undefined,
    });
    console.log(JSON.stringify({ type: "new_york_city_candidate_finance_due_sync", enabled: true, result }, null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("NYC candidate finance due sync failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
