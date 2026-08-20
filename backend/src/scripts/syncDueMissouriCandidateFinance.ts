import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { getPipelineEnv, loadProjectEnv } from "../config/env.js";
import { isMissouriCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { syncDueMissouriCandidateFinance } from "../pipeline/missouriFinance/missouriCandidateFinanceBatchSync.js";
import { parseMissouriCandidateFinanceCliArgs } from "./missouriCandidateFinanceCli.js";

async function main(): Promise<void> {
  loadProjectEnv();
  const args = parseMissouriCandidateFinanceCliArgs(process.argv.slice(2));
  if (!isMissouriCampaignFinanceSyncEnabled(args.force)) throw new Error("Missouri campaign finance sync is disabled");
  const pool = new Pool({ connectionString: getPipelineEnv().DATABASE_URL });
  try {
    console.log(JSON.stringify(await syncDueMissouriCandidateFinance({
      db: pool, dryRun: args.dryRun, forceRawDataRefresh: args.force, maxCandidates: args.maxCandidates,
      staleAfterDays: args.staleAfterDays, electionLookbackDays: args.electionLookbackDays,
      electionLookaheadDays: args.electionLookaheadDays, cacheDir: args.cacheDir,
    }), null, 2));
  } finally {
    await pool.end();
  }
}

if ((process.argv[1] ? pathToFileURL(process.argv[1]).href : null) === import.meta.url) {
  main().catch((error) => { console.error("Missouri campaign finance sync failed:", error); process.exit(1); });
}
