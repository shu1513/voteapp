import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const groups = await pool.query<{
      candidate_id: string;
      projected_v2_key: string;
      row_count: string;
    }>(
      `
        WITH projected AS (
          SELECT
            id,
            candidate_id,
            created_at,
            'v2_' || md5(
              concat_ws(
                '|',
                'v2',
                regexp_replace(lower(btrim(source_url)), '/+$', ''),
                event_date::text,
                btrim(
                  regexp_replace(
                    regexp_replace(lower(btrim(title)), '[^a-z0-9]+', ' ', 'g'),
                    '\\s+',
                    ' ',
                    'g'
                  )
                )
              )
            ) AS projected_v2_key
          FROM public.candidate_records
        )
        SELECT
          candidate_id,
          projected_v2_key,
          COUNT(*)::text AS row_count
        FROM projected
        GROUP BY candidate_id, projected_v2_key
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC, candidate_id ASC
      `
    );

    const examples = await pool.query<{
      candidate_id: string;
      projected_v2_key: string;
      id: string;
      created_at: string;
      event_date: string;
      title: string;
      source_url: string;
    }>(
      `
        WITH projected AS (
          SELECT
            id,
            candidate_id,
            created_at,
            event_date::text AS event_date,
            title,
            source_url,
            'v2_' || md5(
              concat_ws(
                '|',
                'v2',
                regexp_replace(lower(btrim(source_url)), '/+$', ''),
                event_date::text,
                btrim(
                  regexp_replace(
                    regexp_replace(lower(btrim(title)), '[^a-z0-9]+', ' ', 'g'),
                    '\\s+',
                    ' ',
                    'g'
                  )
                )
              )
            ) AS projected_v2_key
          FROM public.candidate_records
        ),
        dup_groups AS (
          SELECT candidate_id, projected_v2_key
          FROM projected
          GROUP BY candidate_id, projected_v2_key
          HAVING COUNT(*) > 1
        )
        SELECT
          p.candidate_id,
          p.projected_v2_key,
          p.id,
          p.created_at::text,
          p.event_date,
          p.title,
          p.source_url
        FROM projected p
        JOIN dup_groups d
          ON d.candidate_id = p.candidate_id
         AND d.projected_v2_key = p.projected_v2_key
        ORDER BY p.candidate_id ASC, p.projected_v2_key ASC, p.created_at ASC, p.id ASC
        LIMIT 100
      `
    );

    const output = {
      type: "candidate_record_v2_key_collision_check",
      ts: new Date().toISOString(),
      collision_group_count: groups.rows.length,
      collision_row_count: groups.rows.reduce((sum, row) => sum + Number.parseInt(row.row_count, 10), 0),
      groups: groups.rows.map((row) => ({
        candidate_id: row.candidate_id,
        projected_v2_key: row.projected_v2_key,
        row_count: Number.parseInt(row.row_count, 10),
      })),
      examples: examples.rows,
      safe_to_apply_migration_066: groups.rows.length === 0,
    };

    console.log(JSON.stringify(output, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("candidate record v2 key collision check failed:", error);
  process.exit(1);
});
