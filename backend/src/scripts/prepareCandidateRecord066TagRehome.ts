import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const startedAt = new Date();
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const mappingResult = await client.query<{
        kept_id: string;
        deleted_id: string;
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
          ),
          ranked AS (
            SELECT
              id,
              candidate_id,
              projected_v2_key,
              row_number() OVER (
                PARTITION BY candidate_id, projected_v2_key
                ORDER BY created_at ASC, id ASC
              ) AS rn
            FROM projected
          ),
          keepers AS (
            SELECT candidate_id, projected_v2_key, id AS kept_id
            FROM ranked
            WHERE rn = 1
          )
          SELECT
            k.kept_id,
            r.id AS deleted_id
          FROM ranked r
          JOIN keepers k
            ON k.candidate_id = r.candidate_id
           AND k.projected_v2_key = r.projected_v2_key
          WHERE r.rn > 1
        `
      );

      const mappingCount = mappingResult.rows.length;

      let insertedTagRows = 0;
      let deletedTagRows = 0;

      if (mappingCount > 0) {
        const insertedTagsResult = await client.query<{ count: string }>(
          `
            WITH mapping AS (
              SELECT *
              FROM (
                VALUES ${mappingResult.rows
                  .map((_, idx) => `($${idx * 2 + 1}::uuid, $${idx * 2 + 2}::uuid)`)
                  .join(",")}
              ) AS m(kept_id, deleted_id)
            ),
            inserted AS (
              INSERT INTO public.candidate_record_area_tags (
                candidate_record_id,
                research_area_id,
                stance,
                created_at,
                updated_at
              )
              SELECT
                m.kept_id,
                t.research_area_id,
                t.stance,
                t.created_at,
                t.updated_at
              FROM mapping m
              JOIN public.candidate_record_area_tags t
                ON t.candidate_record_id = m.deleted_id
              ON CONFLICT (candidate_record_id, research_area_id) DO NOTHING
              RETURNING 1
            )
            SELECT COUNT(*)::text AS count FROM inserted
          `,
          mappingResult.rows.flatMap((row) => [row.kept_id, row.deleted_id])
        );
        insertedTagRows = Number.parseInt(insertedTagsResult.rows[0]?.count ?? "0", 10);

        const deletedTagsResult = await client.query<{ count: string }>(
          `
            WITH mapping AS (
              SELECT *
              FROM (
                VALUES ${mappingResult.rows
                  .map((_, idx) => `($${idx + 1}::uuid)`)
                  .join(",")}
              ) AS d(deleted_id)
            ),
            deleted AS (
              DELETE FROM public.candidate_record_area_tags t
              USING mapping m
              WHERE t.candidate_record_id = m.deleted_id
              RETURNING 1
            )
            SELECT COUNT(*)::text AS count FROM deleted
          `,
          mappingResult.rows.map((row) => row.deleted_id)
        );
        deletedTagRows = Number.parseInt(deletedTagsResult.rows[0]?.count ?? "0", 10);
      }

      await client.query("COMMIT");

      console.log(
        JSON.stringify(
          {
            type: "candidate_record_066_tag_rehome",
            ts: new Date().toISOString(),
            started_at: startedAt.toISOString(),
            mapping_count: mappingCount,
            inserted_tag_rows: insertedTagRows,
            deleted_old_tag_rows: deletedTagRows,
            rehome_applied: mappingCount > 0,
          },
          null,
          2
        )
      );
    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch {
        // no-op
      }
      throw error;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("candidate record 066 tag rehome failed:", error);
  process.exit(1);
});
