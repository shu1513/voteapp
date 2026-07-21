import { Pool } from "pg";
import { getPipelineEnv, loadProjectEnv } from "../config/env.js";
import { isLosAngelesCityCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { syncDueLosAngelesCandidateFinance } from "../pipeline/losAngelesCityFinance/losAngelesCandidateFinanceBatchSync.js";
// Every other syncDue* script loads .env before its flag check; without this
// the flag reads an unloaded environment and the script silently exits
// {"enabled":false} on local runs.
loadProjectEnv();
if (
  !isLosAngelesCityCampaignFinanceSyncEnabled(process.argv.includes("--force"))
) {
  console.log(JSON.stringify({ enabled: false }));
  process.exit(0);
}
const pool = new Pool({ connectionString: getPipelineEnv().DATABASE_URL });
try {
  console.log(
    JSON.stringify(
      await syncDueLosAngelesCandidateFinance({
        db: pool,
        dryRun: process.argv.includes("--dry-run"),
      }),
    ),
  );
} finally {
  await pool.end();
}
