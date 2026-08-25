import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { recomputeRepresentationPowerScores } from "../pipeline/loaders/districtsLoader.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

/**
 * Reruns the representation_power_score recompute against the existing
 * districts table, without the Census reload the full districts loader
 * performs. Use it to roll a scoring-model change out to a database whose
 * population data is already loaded (e.g. prod promotion after a deploy).
 *
 * Local-only by default; set ALLOW_REMOTE_DB_WRITES=1 for a deliberate
 * remote run. Idempotent: a second run updates 0 rows.
 */
async function main(): Promise<void> {
  loadProjectEnv();
  requireLocalDatabaseTarget();

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const client = await pool.connect();
    try {
      const updated = await recomputeRepresentationPowerScores(client);

      // The model's invariant: a statewide row with a positive population is
      // its own anchor, so it must score exactly 50 (a NULL score there means
      // the recompute failed to reach it). Rows without a positive population
      // are legitimately unscored ("unknown") and stay out of the check.
      const statewideOffBaseline = await client.query<{ count: string }>(
        `
          SELECT COUNT(*) AS count
          FROM public.districts
          WHERE district_type = 'statewide'
            AND population IS NOT NULL
            AND population > 0
            AND representation_power_score IS DISTINCT FROM 50.00
        `
      );
      const offBaseline = Number(statewideOffBaseline.rows[0]?.count ?? 0);
      if (offBaseline > 0) {
        // Exit nonzero: a rollout that broke the baseline must not read as a
        // successful run in a log line nobody checks.
        throw new Error(
          `statewide baseline violated: ${offBaseline} positive-population statewide row(s) not scored exactly 50.00 (updated=${updated})`
        );
      }

      console.log(`representation recompute completed updated=${updated} statewide_off_baseline=0`);
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("representation recompute failed:", error);
  process.exit(1);
});
