import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { loadHistoricalContestImportStatus } from "../pipeline/competitiveness/historicalContestImportStatus.js";

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const status = await loadHistoricalContestImportStatus(pool);
    console.log(
      JSON.stringify(
        {
          type: "historical_contest_margins_status",
          ts: new Date().toISOString(),
          ...status,
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("historical contest margin status check failed:", error);
  process.exit(1);
});
