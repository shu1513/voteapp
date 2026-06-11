import { Pool, type PoolClient } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  buildPresidentialCycleSeeds,
  upsertPresidentialCycles,
} from "../pipeline/presidential/presidentialCycles.js";

async function rollbackQuietly(client: PoolClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch (rollbackError) {
    console.error("presidential cycles seed rollback failed:", rollbackError);
  }
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const startedAt = new Date();
  const seeds = buildPresidentialCycleSeeds(startedAt);

  let client: PoolClient | undefined;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    const result = await upsertPresidentialCycles(client, seeds);
    await client.query("COMMIT");

    const years = Array.from(new Set(seeds.map((seed) => seed.electionYear))).sort((a, b) => a - b);
    const output = {
      type: "presidential_cycles_seed",
      ts: new Date().toISOString(),
      started_at: startedAt.toISOString(),
      years,
      total_seed_rows: seeds.length,
      changed: result.changed,
      unchanged: result.unchanged,
    };

    console.log(JSON.stringify(output, null, 2));
  } catch (error) {
    if (client) {
      await rollbackQuietly(client);
    }
    throw error;
  } finally {
    client?.release();
    await pool.end();
  }
}

main().catch((error) => {
  console.error("presidential cycles seed failed:", error);
  process.exit(1);
});
