import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_DRAFT_STREAM,
  STAGING_ITEM_TYPE_ELECTION,
} from "../../config/electionsPipeline.js";
import { ELECTION_DRAFT_SCHEMA_VERSION } from "../../contracts/electionEnrichmentContract.js";
import { ELECTION_PROMPT_VERSION } from "../../contracts/electionEnrichmentContract.js";
import type { ElectionDraftPayload } from "../../types/election.js";

type ProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
};

export type ElectionsProducerResult = {
  runId: string;
  enqueued: number;
  skipped: number;
  failed: number;
  totalCandidates: number;
  dryRun: boolean;
  force: boolean;
};

type DistrictRow = {
  id: string;
  name: string;
  district_type: string;
  state: string;
};

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function buildIngestKey(districtId: string, runYear: number): string {
  return `elections:${districtId}:${runYear}`;
}

function toDraftPayload(row: DistrictRow): ElectionDraftPayload {
  return {
    district_id: row.id,
    district_name: row.name,
    district_type: row.district_type as ElectionDraftPayload["district_type"],
    state: row.state,
  };
}

export async function runElectionsProducer(options: ProducerOptions = {}): Promise<ElectionsProducerResult> {
  const { dryRun = false, force = false } = options;
  const env = getPipelineEnv();
  const runYear = new Date().getUTCFullYear();
  const runId = `elections_${new Date().toISOString()}`;

  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  await redis.connect();

  let enqueued = 0;
  let skipped = 0;
  let failed = 0;

  try {
    const districtsResult = await pool.query<DistrictRow>(
      `
        SELECT id, name, district_type, state
        FROM public.districts
        ORDER BY district_type, state_fips, geoid_compact
      `
    );
    const drafts = districtsResult.rows.map(toDraftPayload);

    if (dryRun) {
      console.log(`[DRY RUN] elections producer fetched ${drafts.length} district drafts`);
      return {
        runId,
        enqueued: 0,
        skipped: 0,
        failed: 0,
        totalCandidates: drafts.length,
        dryRun: true,
        force,
      };
    }

    for (const draft of drafts) {
      const ingestKey = buildIngestKey(draft.district_id, runYear);
      const serializedPayload = JSON.stringify(draft);
      try {
        const result = await pool.query(
          `
            INSERT INTO staging_items
              (ingest_key, item_type, payload, status, reason, run_id, model, schema_version, prompt_version)
            VALUES
              ($1, $2, $3::jsonb, 'pending', NULL, $4, $5, $6, $7)
            ON CONFLICT (ingest_key) DO UPDATE SET
              item_type = EXCLUDED.item_type,
              payload = EXCLUDED.payload,
              status = 'pending',
              reason = NULL,
              failure_debug = NULL,
              ai_raw_debug = NULL,
              run_id = EXCLUDED.run_id,
              model = EXCLUDED.model,
              schema_version = EXCLUDED.schema_version,
              prompt_version = EXCLUDED.prompt_version,
              validated_at = NULL,
              written_at = NULL,
              updated_at = now()
            WHERE $8::boolean = true OR staging_items.status IN ('failed', 'rejected')
            RETURNING ingest_key
          `,
          [
            ingestKey,
            STAGING_ITEM_TYPE_ELECTION,
            serializedPayload,
            runId,
            `${env.AI_PROVIDER}:${env.AI_MODEL}`,
            ELECTION_DRAFT_SCHEMA_VERSION,
            ELECTION_PROMPT_VERSION,
            force,
          ]
        );

        if (result.rowCount === 0) {
          skipped += 1;
          continue;
        }

        await redis.xAdd(STAGING_DRAFT_STREAM, "*", {
          ingest_key: ingestKey,
          item_type: STAGING_ITEM_TYPE_ELECTION,
          run_id: runId,
          payload: serializedPayload,
        });
        enqueued += 1;
      } catch (error) {
        failed += 1;
        const reason = toReason(error);
        await pool.query(
          `
            INSERT INTO staging_items
              (ingest_key, item_type, payload, status, reason, run_id, model, schema_version, prompt_version)
            VALUES
              ($1, $2, $3::jsonb, 'failed', $4, $5, $6, $7, $8)
            ON CONFLICT (ingest_key) DO UPDATE SET
              status = 'failed',
              reason = EXCLUDED.reason,
              run_id = EXCLUDED.run_id,
              model = EXCLUDED.model,
              schema_version = EXCLUDED.schema_version,
              prompt_version = EXCLUDED.prompt_version,
              updated_at = now()
          `,
          [
            ingestKey,
            STAGING_ITEM_TYPE_ELECTION,
            serializedPayload,
            reason,
            runId,
            `${env.AI_PROVIDER}:${env.AI_MODEL}`,
            ELECTION_DRAFT_SCHEMA_VERSION,
            ELECTION_PROMPT_VERSION,
          ]
        );
      }
    }

    return {
      runId,
      enqueued,
      skipped,
      failed,
      totalCandidates: drafts.length,
      dryRun: false,
      force,
    };
  } finally {
    await redis.quit();
    await pool.end();
  }
}
