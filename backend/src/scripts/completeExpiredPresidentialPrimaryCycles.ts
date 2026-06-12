import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { completeExpiredPresidentialPrimaryCycles } from "../pipeline/presidential/presidentialPrimaryCycleCompletion.js";

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const dryRun = process.argv.includes("--dry-run");

  try {
    const result = await completeExpiredPresidentialPrimaryCycles(pool, { dryRun });
    console.log(JSON.stringify(result, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("complete expired presidential primary cycles failed:", error);
  process.exit(1);
});
