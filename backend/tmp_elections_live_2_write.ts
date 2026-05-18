import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "./src/config/env.js";
import { buildEnrichElectionsConfigFromEnv, enrichElections } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION, ELECTION_ENRICHMENT_SCHEMA_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import { runElectionsValidator } from "./src/pipeline/validators/electionsValidator.js";
import { runElectionsWriter } from "./src/pipeline/writers/electionsWriter.js";
import { STAGING_ITEM_TYPE_ELECTION, STAGING_PENDING_STREAM } from "./src/config/electionsPipeline.js";
import type { ElectionDraftPayload } from "./src/types/election.js";

type DistrictRow = { id: string; name: string; district_type: ElectionDraftPayload["district_type"]; state: string };

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const config = buildEnrichElectionsConfigFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  await redis.connect();

  const runId = `elections_live2_${new Date().toISOString()}`;

  try {
    const districts = await pool.query<DistrictRow>(
      `
      SELECT id, name, district_type, state
      FROM public.districts
      WHERE (district_type = 'statewide' AND state = 'VT' AND lower(name) = 'vermont')
         OR (district_type = 'county' AND state = 'CA' AND lower(name) LIKE '%los angeles county%')
      ORDER BY district_type
      `
    );

    if (districts.rowCount !== 2) {
      throw new Error(`Expected 2 districts for elections live2 run; got ${districts.rowCount ?? 0}`);
    }

    const stagingStatus: Array<Record<string, unknown>> = [];

    for (const row of districts.rows) {
      const draft: ElectionDraftPayload = {
        district_id: row.id,
        district_name: row.name,
        district_type: row.district_type,
        state: row.state,
      };

      const live = await enrichElections(
        {
          ingestKey: `${runId}:enrich:${row.id}`,
          draft,
          promptVersion: ELECTION_PROMPT_VERSION,
          softRetryCount: 0,
          reviewFeedback: [],
        },
        config,
        [{ provider: "claude", model: "claude-sonnet-4-6" }]
      );

      if (!live.ok) {
        stagingStatus.push({
          district_id: row.id,
          district_name: row.name,
          ok: false,
          errorCode: live.errorCode,
          reason: live.reason,
          retryable: live.retryable,
        });
        continue;
      }

      const ingestKey = `${runId}:row:${row.id}`;

      await pool.query(
        `
          INSERT INTO staging_items
            (ingest_key, item_type, payload, status, reason, run_id, model, schema_version, prompt_version, failure_debug, ai_raw_debug, validated_at, written_at)
          VALUES
            ($1, $2, $3::jsonb, 'pending', NULL, $4, $5, $6, $7, NULL, $8::jsonb, NULL, NULL)
          ON CONFLICT (ingest_key) DO UPDATE SET
            payload = EXCLUDED.payload,
            status = 'pending',
            reason = NULL,
            run_id = EXCLUDED.run_id,
            model = EXCLUDED.model,
            schema_version = EXCLUDED.schema_version,
            prompt_version = EXCLUDED.prompt_version,
            failure_debug = NULL,
            ai_raw_debug = EXCLUDED.ai_raw_debug,
            validated_at = NULL,
            written_at = NULL,
            updated_at = now()
        `,
        [
          ingestKey,
          STAGING_ITEM_TYPE_ELECTION,
          JSON.stringify(live.payload),
          runId,
          `${live.provider}:${live.model}`,
          ELECTION_ENRICHMENT_SCHEMA_VERSION,
          live.promptVersion,
          JSON.stringify(live.aiRawDebug ?? {}),
        ]
      );

      await redis.xAdd(STAGING_PENDING_STREAM, "*", {
        ingest_key: ingestKey,
        item_type: STAGING_ITEM_TYPE_ELECTION,
        run_id: runId,
        payload: JSON.stringify(live.payload),
      });

      stagingStatus.push({
        district_id: row.id,
        district_name: row.name,
        ok: true,
        provider: live.provider,
        model: live.model,
        entriesCount: live.payload.entries.length,
      });
    }

    const foreignWorkload = await pool.query<{
      status: string;
      count: string;
    }>(
      `
      SELECT status, COUNT(*)::text AS count
      FROM staging_items
      WHERE item_type = 'election'
        AND status IN ('pending', 'validated')
        AND run_id IS DISTINCT FROM $1
      GROUP BY status
      `,
      [runId]
    );
    if ((foreignWorkload.rowCount ?? 0) > 0) {
      throw new Error(
        `Refusing shared-stream run: found foreign election workload in pending/validated states (${foreignWorkload.rows
          .map((r) => `${r.status}=${r.count}`)
          .join(", ")})`
      );
    }

    await runElectionsValidator({ once: true, batchSize: 20, blockMs: 1000 });
    await runElectionsWriter({ once: true, batchSize: 20, blockMs: 1000 });

    const stagingRows = await pool.query<{
      ingest_key: string;
      status: string;
      reason: string | null;
      model: string | null;
    }>(
      `
      SELECT ingest_key, status, reason, model
      FROM staging_items
      WHERE item_type = 'election' AND run_id = $1
      ORDER BY ingest_key
      `,
      [runId]
    );

    const writtenDistrictIds = stagingRows.rows
      .filter((row) => row.status === "written")
      .map((row) => row.ingest_key.split(":row:")[1])
      .filter((id): id is string => typeof id === "string" && id.length > 0);

    const written = await pool.query<{
      district_id: string;
      official_ballot_title: string;
      election_date: string;
      race_type: string;
      sources: unknown;
    }>(
      `
      SELECT district_id::text, official_ballot_title, election_date::text, race_type::text, sources
      FROM public.elections
      WHERE district_id = ANY($1::uuid[])
      ORDER BY district_id, election_date, official_ballot_title
      `,
      [writtenDistrictIds]
    );

    console.log(
      JSON.stringify(
        {
          type: "elections_live2_write_summary",
          runId,
          stagingStatus,
          stagingRows: stagingRows.rows,
          writtenCount: written.rowCount ?? 0,
          writtenRows: written.rows,
        },
        null,
        2
      )
    );
  } finally {
    if (redis.isOpen) {
      await redis.quit();
    }
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});
