import { Pool } from 'pg';
import { createClient } from 'redis';
import { getPipelineEnv } from './src/config/env.ts';
import {
  STAGING_DRAFT_STREAM,
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STATE_RESOURCE_SEED_SOURCES,
  STATE_ABBREVIATION_REFERENCE_URL,
} from './src/config/stateResourcePipeline.ts';
import { STATE_RESOURCE_DRAFT_SCHEMA_VERSION } from './src/contracts/stateResourceEnrichmentContract.ts';
import { runStateResourcesEnricher } from './src/pipeline/enrichers/stateResourcesEnricher.ts';
import { runStateResourcesValidator } from './src/pipeline/validators/stateResourcesValidator.ts';
import { runStateResourcesWriter } from './src/pipeline/writers/stateResourcesWriter.ts';

type StateRow = { state_fips: string; state_abbreviation: string; state_name: string };

(async () => {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const runId = `live2_${new Date().toISOString()}`;
  const runYear = new Date().getUTCFullYear();

  try {
    await redis.connect();

    const states = await pool.query<StateRow>(
      `SELECT state_fips, state_abbreviation, state_name FROM state_resources WHERE state_fips IN ('04','05') ORDER BY state_fips`
    );

    for (const s of states.rows) {
      const ingestKey = `state_resources:${s.state_fips}:${runYear}`;
      const payload = {
        state_fips: s.state_fips,
        state_abbreviation: s.state_abbreviation,
        state_name: s.state_name,
        population_estimate: null,
        census_source_url: 'https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*',
        state_abbreviation_reference_url: STATE_ABBREVIATION_REFERENCE_URL,
        seed_sources: [...STATE_RESOURCE_SEED_SOURCES],
        allow_open_web_research: true,
      };

      await pool.query(
        `
        INSERT INTO staging_items
          (ingest_key, item_type, payload, status, reason, run_id, model, schema_version, prompt_version)
        VALUES
          ($1, $2, $3::jsonb, 'pending', NULL, $4, NULL, $5, $6)
        ON CONFLICT (ingest_key) DO UPDATE SET
          item_type = EXCLUDED.item_type,
          payload = EXCLUDED.payload,
          status = 'pending',
          reason = NULL,
          failure_debug = NULL,
          ai_raw_debug = NULL,
          run_id = EXCLUDED.run_id,
          model = NULL,
          schema_version = EXCLUDED.schema_version,
          prompt_version = EXCLUDED.prompt_version,
          validated_at = NULL,
          written_at = NULL,
          updated_at = now()
        `,
        [
          ingestKey,
          STAGING_ITEM_TYPE_STATE_RESOURCES,
          JSON.stringify(payload),
          runId,
          STATE_RESOURCE_DRAFT_SCHEMA_VERSION,
          env.PROMPT_VERSION,
        ]
      );

      await redis.xAdd(STAGING_DRAFT_STREAM, '*', {
        ingest_key: ingestKey,
        item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
        run_id: runId,
        payload: JSON.stringify(payload),
      });
    }

    await runStateResourcesEnricher({ once: true, batchSize: 20, blockMs: 1000 });
    await runStateResourcesValidator({ once: true, batchSize: 20, blockMs: 1000 });
    await runStateResourcesWriter({ once: true, batchSize: 20, blockMs: 1000 });

    const status = await pool.query(
      `SELECT ingest_key,status,model,reason FROM staging_items WHERE item_type='state_resources' AND run_id=$1 ORDER BY ingest_key`,
      [runId]
    );

    const writtenRows = await pool.query(
      `SELECT state_fips,polling_hours,sources->'polling_hours' AS polling_hours_sources FROM state_resources WHERE state_fips IN ('04','05') ORDER BY state_fips`
    );

    console.log(JSON.stringify({ type: 'live2_summary', runId, status: status.rows, writtenRows: writtenRows.rows }, null, 2));
  } finally {
    if (redis.isOpen) await redis.quit();
    await pool.end();
  }
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
