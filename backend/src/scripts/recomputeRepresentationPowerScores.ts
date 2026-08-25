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

      // Sanity check the model's invariant: every scored statewide row is
      // its own anchor, so each must sit exactly on the 50 baseline.
      const statewideOffBaseline = await client.query<{ count: string }>(
        `
          SELECT COUNT(*) AS count
          FROM public.districts
          WHERE district_type = 'statewide'
            AND representation_power_score IS DISTINCT FROM 50.00
        `
      );

      console.log(
        `representation recompute completed updated=${updated} statewide_off_baseline=${statewideOffBaseline.rows[0]?.count ?? "?"}`
      );
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
