import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { loadPresidentialPrimaryDateResearchSchedulerState } from "../scheduler/presidentialPrimaryDateResearchScheduler.js";

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const state = await loadPresidentialPrimaryDateResearchSchedulerState(pool);
    console.log(
      JSON.stringify(
        {
          type: "presidential_primary_date_research_scheduler_status",
          ts: new Date().toISOString(),
          state,
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
  console.error("presidential primary date research scheduler status failed:", error);
  process.exit(1);
});
