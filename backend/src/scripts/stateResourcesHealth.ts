import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";

function readHoursArg(): number {
  const arg = process.argv.find((value) => value.startsWith("--hours="));
  if (!arg) {
    return 24;
  }

  const value = Number.parseInt(arg.slice("--hours=".length), 10);
  if (!Number.isFinite(value) || value <= 0) {
    return 24;
  }
  return value;
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const hours = readHoursArg();

  try {
    const statusResult = await pool.query<{
      status: string;
      count: string;
    }>(
      `
        SELECT status, COUNT(*)::text AS count
        FROM staging_items
        WHERE item_type = 'state_resources'
          AND updated_at >= now() - ($1::text || ' hours')::interval
        GROUP BY status
        ORDER BY status
      `,
      [hours]
    );

    const reasonResult = await pool.query<{
      reason_bucket: string;
      count: string;
    }>(
      `
        SELECT
          CASE
            WHEN reason IS NULL OR btrim(reason) = '' THEN 'NONE'
            WHEN reason LIKE '[CONFLICT_WARN]%' THEN '[CONFLICT_WARN]'
            WHEN reason ~ '^\\[[A-Z0-9_ -]{2,60}\\]' THEN substring(reason from '^\\[[A-Z0-9_ -]{2,60}\\]')
            ELSE left(reason, 80)
          END AS reason_bucket,
          COUNT(*)::text AS count
        FROM staging_items
        WHERE item_type = 'state_resources'
          AND updated_at >= now() - ($1::text || ' hours')::interval
        GROUP BY reason_bucket
        ORDER BY COUNT(*) DESC
        LIMIT 20
      `,
      [hours]
    );

    const output = {
      type: "pipeline_health",
      ts: new Date().toISOString(),
      window_hours: hours,
      by_status: statusResult.rows.map((row) => ({
        status: row.status,
        count: Number.parseInt(row.count, 10),
      })),
      top_reason_buckets: reasonResult.rows.map((row) => ({
        reason_bucket: row.reason_bucket,
        count: Number.parseInt(row.count, 10),
      })),
    };

    console.log(JSON.stringify(output, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("state_resources health check failed:", error);
  process.exit(1);
});
