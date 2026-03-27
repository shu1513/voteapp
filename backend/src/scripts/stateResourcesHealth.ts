import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { bucketReasonForObservability } from "../pipeline/utils/observability.js";

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
      reason: string | null;
      count: string;
    }>(
      `
        SELECT
          reason,
          COUNT(*)::text AS count
        FROM staging_items
        WHERE item_type = 'state_resources'
          AND updated_at >= now() - ($1::text || ' hours')::interval
        GROUP BY reason
        ORDER BY COUNT(*) DESC
      `,
      [hours]
    );

    const reasonBuckets = new Map<string, number>();
    for (const row of reasonResult.rows) {
      const bucket = bucketReasonForObservability(row.reason ?? "");
      const count = Number.parseInt(row.count, 10);
      reasonBuckets.set(bucket, (reasonBuckets.get(bucket) ?? 0) + count);
    }

    const topReasonBuckets = Array.from(reasonBuckets.entries())
      .map(([reason_bucket, count]) => ({ reason_bucket, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 20);

    const output = {
      type: "pipeline_health",
      ts: new Date().toISOString(),
      window_hours: hours,
      by_status: statusResult.rows.map((row) => ({
        status: row.status,
        count: Number.parseInt(row.count, 10),
      })),
      top_reason_buckets: topReasonBuckets,
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
