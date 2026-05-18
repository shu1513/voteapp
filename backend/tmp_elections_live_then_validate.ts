import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "./src/config/env.js";
import { buildEnrichElectionsConfigFromEnv, enrichElections } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION, ELECTION_ENRICHMENT_SCHEMA_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import { runElectionsValidator } from "./src/pipeline/validators/electionsValidator.js";
import { STAGING_ITEM_TYPE_ELECTION, STAGING_PENDING_STREAM } from "./src/config/electionsPipeline.js";
import type { ElectionDraftPayload } from "./src/types/election.js";

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const config = buildEnrichElectionsConfigFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  try {
    await redis.connect();
    const runId = `live_then_validate_${new Date().toISOString()}`;

    const district = await pool.query<{ id: string; name: string; district_type: string; state: string }>(
      `SELECT id, name, district_type, state
       FROM public.districts
       WHERE district_type = 'statewide' AND state = 'VT' AND lower(name) = 'vermont'
       LIMIT 1`
    );

    const row = district.rows[0];
    if (!row) {
      throw new Error("Missing Vermont statewide district row");
    }

    const draft: ElectionDraftPayload = {
      district_id: row.id,
      district_name: row.name,
      district_type: row.district_type as ElectionDraftPayload["district_type"],
      state: row.state,
    };

    const live = await enrichElections(
      {
        ingestKey: `${runId}:live_enrich`,
        draft,
        promptVersion: ELECTION_PROMPT_VERSION,
        softRetryCount: 0,
        reviewFeedback: [],
      },
      config,
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    if (!live.ok) {
      console.log(JSON.stringify({ type: "elections_live_then_validate", ok: false, phase: "enrich", failure: live }, null, 2));
      return;
    }

    const ingestKey = `${runId}:from_live`;

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

    await runElectionsValidator({ once: true, batchSize: 10, blockMs: 1000 });

    const status = await pool.query<{ status: string; reason: string | null }>(
      `SELECT status, reason FROM staging_items WHERE ingest_key = $1`,
      [ingestKey]
    );

    console.log(
      JSON.stringify(
        {
          type: "elections_live_then_validate",
          ok: true,
          district: draft,
          provider: live.provider,
          model: live.model,
          entriesCount: live.payload.entries.length,
          entries: live.payload.entries,
          finalStatus: status.rows[0] ?? null,
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
