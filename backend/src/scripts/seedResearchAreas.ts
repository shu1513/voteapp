import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Pool, type PoolClient } from "pg";

import { getPipelineEnv } from "../config/env.js";

function getSeedFilePath(): string {
  const scriptDir = dirname(fileURLToPath(import.meta.url));
  return resolve(scriptDir, "../../../db/seeds/research_areas_v1.sql");
}

function sumAffectedRows(result: unknown): number {
  const results = Array.isArray(result) ? result : [result];
  let total = 0;
  for (const item of results) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const rowCount = (item as { rowCount?: unknown }).rowCount;
    if (typeof rowCount === "number" && Number.isFinite(rowCount) && rowCount > 0) {
      total += rowCount;
    }
  }
  return total;
}

async function main(): Promise<void> {
  const seedFilePath = getSeedFilePath();
  const sql = await readFile(seedFilePath, "utf8");
  if (sql.trim().length === 0) {
    throw new Error(`Seed SQL file is empty: ${seedFilePath}`);
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const startedAt = new Date();

  let client: PoolClient | undefined;
  let rowCount = 0;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    const result = await client.query(sql);
    rowCount = sumAffectedRows(result);
    await client.query("COMMIT");
  } catch (error) {
    if (client) {
      await client.query("ROLLBACK");
    }
    throw error;
  } finally {
    client?.release();
    await pool.end();
  }

  const output = {
    type: "research_areas_seed",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    seed_file: seedFilePath,
    affected_rows: rowCount,
  };
  console.log(JSON.stringify(output, null, 2));
}

main().catch((error) => {
  console.error("research areas seed failed:", error);
  process.exit(1);
});
