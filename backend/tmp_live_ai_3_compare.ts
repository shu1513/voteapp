import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "./src/config/env.ts";
import {
  STAGING_DRAFT_STREAM,
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STATE_ABBREVIATION_REFERENCE_URL,
  STATE_RESOURCE_SEED_SOURCES,
} from "./src/config/stateResourcePipeline.ts";
import { STATE_RESOURCE_DRAFT_SCHEMA_VERSION } from "./src/contracts/stateResourceEnrichmentContract.ts";
import { runStateResourcesEnricher } from "./src/pipeline/enrichers/stateResourcesEnricher.ts";
import { runStateResourcesValidator } from "./src/pipeline/validators/stateResourcesValidator.ts";
import { runStateResourcesWriter } from "./src/pipeline/writers/stateResourcesWriter.ts";
import { runStateResourcesRetrySweeper } from "./src/pipeline/retries/stateResourcesRetry.ts";

type StateRow = { state_fips: string; state_abbreviation: string; state_name: string };

async function run(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const runId = `live3_${new Date().toISOString()}`;
  const runYear = new Date().getUTCFullYear();
  const targetCount = 3;
  const startedAt = Date.now();

  const fetchStatus = async () => {
    const result = await pool.query<{ status: string; count: string }>(
      `
      SELECT status, COUNT(*)::text AS count
      FROM staging_items
      WHERE item_type = 'state_resources'
        AND run_id = $1
      GROUP BY status
      ORDER BY status
      `,
      [runId]
    );

    const map = new Map<string, number>();
    for (const row of result.rows) {
      map.set(row.status, Number.parseInt(row.count, 10));
    }

    return {
      pending: map.get("pending") ?? 0,
      validated: map.get("validated") ?? 0,
      written: map.get("written") ?? 0,
      failed: map.get("failed") ?? 0,
      rejected: map.get("rejected") ?? 0,
      requeueing: map.get("requeueing") ?? 0,
      raw: Object.fromEntries(map.entries()),
    };
  };

  try {
    await redis.connect();

    const states = await pool.query<StateRow>(
      `
      SELECT state_fips, state_abbreviation, state_name
      FROM state_resources
      ORDER BY state_fips
      LIMIT $1
      `,
      [targetCount]
    );

    if (states.rowCount !== targetCount) {
      throw new Error(`Expected ${targetCount} states in state_resources, found ${states.rowCount ?? 0}`);
    }

    for (const state of states.rows) {
      const ingestKey = `state_resources:${state.state_fips}:${runYear}`;
      const payload = {
        state_fips: state.state_fips,
        state_abbreviation: state.state_abbreviation,
        state_name: state.state_name,
        population_estimate: null,
        census_source_url:
          "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*",
        state_abbreviation_reference_url: STATE_ABBREVIATION_REFERENCE_URL,
        seed_sources: [...STATE_RESOURCE_SEED_SOURCES],
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

      await redis.xAdd(STAGING_DRAFT_STREAM, "*", {
        ingest_key: ingestKey,
        item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
        run_id: runId,
        payload: JSON.stringify(payload),
      });
    }

    let completed = false;
    const maxRounds = 8;
    for (let round = 1; round <= maxRounds; round += 1) {
      const roundStarted = Date.now();
      await runStateResourcesEnricher({ once: true, batchSize: 100, blockMs: 1000 });
      await runStateResourcesValidator({ once: true, batchSize: 100, blockMs: 1000 });
      await runStateResourcesWriter({ once: true, batchSize: 100, blockMs: 1000 });
      const status = await fetchStatus();
      const done = status.written === targetCount && status.pending === 0 && status.validated === 0;

      console.log(
        JSON.stringify({
          type: "round_status",
          runId,
          round,
          elapsedMs: Date.now() - roundStarted,
          status,
        })
      );

      if (done) {
        completed = true;
        break;
      }

      if (status.failed > 0 || status.rejected > 0 || status.requeueing > 0) {
        const retry = await runStateResourcesRetrySweeper({ maxItems: 200 });
        console.log(JSON.stringify({ type: "retry_sweep", runId, round, retry }));
      }
    }

    const finalStatus = await fetchStatus();
    const failures = await pool.query<{
      ingest_key: string;
      model: string | null;
      status: string;
      reason: string | null;
    }>(
      `
      SELECT ingest_key, model, status, reason
      FROM staging_items
      WHERE item_type = 'state_resources'
        AND run_id = $1
        AND status IN ('failed','rejected')
      ORDER BY ingest_key
      `,
      [runId]
    );

    const modelUsage = await pool.query<{ model: string | null; count: string }>(
      `
      SELECT model, COUNT(*)::text AS count
      FROM staging_items
      WHERE item_type = 'state_resources'
        AND run_id = $1
      GROUP BY model
      ORDER BY COUNT(*) DESC
      `,
      [runId]
    );

    console.log(
      JSON.stringify({
        type: "final_status",
        runId,
        totalElapsedMs: Date.now() - startedAt,
        completed,
        finalStatus,
        modelUsage: modelUsage.rows.map((r) => ({
          model: r.model,
          count: Number.parseInt(r.count, 10),
        })),
        failures: failures.rows,
      })
    );

    if (!completed) {
      throw new Error(`Run ${runId} did not complete within ${maxRounds} rounds`);
    }
  } finally {
    if (redis.isOpen) {
      await redis.quit();
    }
    await pool.end();
  }
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
