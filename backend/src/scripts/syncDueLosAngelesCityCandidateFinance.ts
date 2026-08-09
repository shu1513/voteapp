import { Pool } from "pg";
import { getPipelineEnv, loadProjectEnv } from "../config/env.js";
import { isLosAngelesCityCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { syncDueLosAngelesCandidateFinance } from "../pipeline/losAngelesCityFinance/losAngelesCandidateFinanceBatchSync.js";
const KNOWN_FLAGS = new Set(["--dry-run", "--force"]);
// Strict like the Ohio/Georgia sync-due CLIs: a typo (--dryrun) or bare
// positional ("dry-run" after npm's own "--" separator) must fail loudly
// instead of silently running a REAL sync.
for (const arg of process.argv.slice(2)) {
  if (!KNOWN_FLAGS.has(arg)) {
    throw new Error(
      `Unknown Los Angeles City candidate finance due sync flag: ${arg}`,
    );
  }
}
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
