import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import { STAGING_DRAFT_STREAM, STAGING_ITEM_TYPE_ELECTION } from "../../config/electionsPipeline.js";
import {
  ELECTION_DRAFT_SCHEMA_VERSION,
  ELECTION_PROMPT_VERSION,
} from "../../contracts/electionEnrichmentContract.js";
import type { ElectionDraftPayload } from "../../types/election.js";
import {
  listDistrictElectionSearchEligibility,
  type DistrictElectionSearchEligibilityRow,
} from "../elections/electionsSearchEligibility.js";
import { readElectionsSearchPolicyFromEnv } from "../elections/electionsSearchPolicy.js";

type ProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
  maxEnqueuePerRun?: number;
};

export type ElectionsSearchRolloverProducerResult = {
  enabled: boolean;
  asOfDate: string;
  cooldownDays: number;
  maxEnqueuePerRun: number;
  districts_scanned: number;
  due_count: number;
  due_overflow_count: number;
  enqueued_count: number;
  skipped_cooldown: number;
  skipped_not_due: number;
  max_enqueue_hit: boolean;
  failed_count: number;
  dryRun: boolean;
  force: boolean;
};

function buildIngestKey(districtId: string, runYear: number): string {
  return `elections:${districtId}:${runYear}`;
}

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function toDraftPayload(row: DistrictElectionSearchEligibilityRow): ElectionDraftPayload {
  return {
    district_id: row.district_id,
    district_name: row.district_name,
    district_type: row.district_type,
    state: row.state,
  };
}

function isDueReason(reason: DistrictElectionSearchEligibilityRow["reason"]): boolean {
  return reason === "never_searched" || reason === "due_no_upcoming";
}

export async function runElectionsSearchRolloverProducer(
  options: ProducerOptions = {}
): Promise<ElectionsSearchRolloverProducerResult> {
  const { dryRun = false, force = false } = options;
  const policy = readElectionsSearchPolicyFromEnv();
  const maxEnqueuePerRun = options.maxEnqueuePerRun ?? policy.maxEnqueuePerRun;
  const enabled = force || policy.enabled;

  if (!enabled) {
    console.log(
      `elections search rollover producer skipped: disabled by flag (as_of=${policy.asOfDate})`
    );
    return {
      enabled: false,
      asOfDate: policy.asOfDate,
      cooldownDays: policy.cooldownDays,
      maxEnqueuePerRun,
      districts_scanned: 0,
      due_count: 0,
      due_overflow_count: 0,
      enqueued_count: 0,
      skipped_cooldown: 0,
      skipped_not_due: 0,
      max_enqueue_hit: false,
      failed_count: 0,
      dryRun,
      force,
    };
  }

  const env = getPipelineEnv();
  const runYear = new Date().getUTCFullYear();
  const runId = `elections_search_rollover_${new Date().toISOString()}`;
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  try {
    const rows = await listDistrictElectionSearchEligibility(pool, {
      asOfDate: policy.asOfDate,
      cooldownDays: policy.cooldownDays,
    });

    const districts_scanned = rows.length;
    const skipped_cooldown = rows.filter((row) => row.reason === "cooldown_not_elapsed").length;
    const skipped_not_due = rows.filter((row) => row.reason === "not_due").length;
    const dueRows = rows.filter((row) => isDueReason(row.reason));
    const due_count = dueRows.length;
    const selectedDueRows = dueRows.slice(0, maxEnqueuePerRun);
    const max_enqueue_hit = dueRows.length > selectedDueRows.length;
    const due_overflow_count = Math.max(0, dueRows.length - selectedDueRows.length);

    if (dryRun) {
      return {
        enabled: true,
        asOfDate: policy.asOfDate,
        cooldownDays: policy.cooldownDays,
        maxEnqueuePerRun,
        districts_scanned,
        due_count,
        due_overflow_count,
        enqueued_count: 0,
        skipped_cooldown,
        skipped_not_due,
        max_enqueue_hit,
        failed_count: 0,
        dryRun: true,
        force,
      };
    }

    await redis.connect();
    let enqueued_count = 0;
    let failed_count = 0;

    for (const row of selectedDueRows) {
      const draft = toDraftPayload(row);
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
            WHERE $8::boolean = true OR staging_items.status IN ('failed', 'rejected', 'written', 'no_results')
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
          continue;
        }

        await redis.xAdd(STAGING_DRAFT_STREAM, "*", {
          ingest_key: ingestKey,
          item_type: STAGING_ITEM_TYPE_ELECTION,
          run_id: runId,
          payload: serializedPayload,
        });
        enqueued_count += 1;
      } catch (error) {
        failed_count += 1;
        const reason = toReason(error);
        try {
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
        } catch (persistError) {
          console.error("elections search rollover failed to persist failed staging item:", {
            runId,
            ingestKey,
            originalReason: reason,
            persistReason: toReason(persistError),
          });
        }
      }
    }

    return {
      enabled: true,
      asOfDate: policy.asOfDate,
      cooldownDays: policy.cooldownDays,
      maxEnqueuePerRun,
      districts_scanned,
      due_count,
      due_overflow_count,
      enqueued_count,
      skipped_cooldown,
      skipped_not_due,
      max_enqueue_hit,
      failed_count,
      dryRun: false,
      force,
    };
  } finally {
    const settled = await Promise.allSettled([
      redis.isOpen ? redis.quit() : Promise.resolve(),
      pool.end(),
    ]);
    for (const result of settled) {
      if (result.status === "rejected") {
        console.error("elections search rollover producer cleanup warning:", toReason(result.reason));
      }
    }
  }
}
