import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";

function readLimitArg(): number {
  const arg = process.argv.find((value) => value.startsWith("--limit="));
  if (!arg) {
    return 100;
  }

  const parsed = Number.parseInt(arg.slice("--limit=".length), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 100;
  }
  return parsed;
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const limit = readLimitArg();

  try {
    const result = await pool.query<{
      district_type: string;
      official_ballot_title: string;
      count: string;
    }>(
      `
        SELECT
          d.district_type,
          e.official_ballot_title,
          COUNT(*)::text AS count
        FROM public.elections AS e
        JOIN public.districts AS d
          ON d.id = e.district_id
        WHERE e.race_type = 'office'
          AND e.office_id IS NULL
        GROUP BY d.district_type, e.official_ballot_title
        ORDER BY COUNT(*) DESC, d.district_type ASC, e.official_ballot_title ASC
        LIMIT $1::int
      `,
      [limit]
    );

    const output = {
      type: "unmatched_offices_health",
      ts: new Date().toISOString(),
      limit,
      groups_returned: result.rows.length,
      total_unmatched: result.rows.reduce(
        (sum, row) => sum + Number.parseInt(row.count, 10),
        0
      ),
      rows: result.rows.map((row) => ({
        district_type: row.district_type,
        official_ballot_title: row.official_ballot_title,
        count: Number.parseInt(row.count, 10),
      })),
    };

    console.log(JSON.stringify(output, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("unmatched offices health check failed:", error);
  process.exit(1);
});
