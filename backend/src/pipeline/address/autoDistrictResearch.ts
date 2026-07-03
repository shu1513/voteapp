import type { Pool, PoolClient } from "pg";

import { getPipelineEnv } from "../../config/env.js";
import { STAGING_DRAFT_STREAM, STAGING_ITEM_TYPE_ELECTION } from "../../config/electionsPipeline.js";
import { isAutoDistrictResearchEnabled } from "../../config/featureFlags.js";
import {
  ELECTION_DRAFT_SCHEMA_VERSION,
  ELECTION_PROMPT_VERSION,
} from "../../contracts/electionEnrichmentContract.js";
import type { ElectionDraftPayload } from "../../types/election.js";
import { readElectionsSearchPolicyFromEnv } from "../elections/electionsSearchPolicy.js";
import type { AddressResolvedDistrict } from "./addressDistrictLookup.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type AutoDistrictResearchConfig = {
  enabled: boolean;
  ttlDays: number;
};

export function readAutoDistrictResearchConfigFromEnv(): AutoDistrictResearchConfig {
  return {
    enabled: isAutoDistrictResearchEnabled(),
    ttlDays: readElectionsSearchPolicyFromEnv().cooldownDays,
  };
}

export type AutoDistrictResearchRedis = {
  isOpen: boolean;
  xAdd: (key: string, id: string, message: Record<string, string>) => Promise<unknown>;
};

export type AutoDistrictResearchResult = {
  checked: number;
  enqueued: string[];
  skipped_fresh: number;
  skipped_claimed: number;
  failed: number;
};

export type AutoDistrictResearchTrigger = (
  districts: readonly AddressResolvedDistrict[]
) => Promise<AutoDistrictResearchResult>;

type TriggerDeps = {
  db: Queryable;
  getRedis: () => AutoDistrictResearchRedis | null;
  config: AutoDistrictResearchConfig;
  now?: () => Date;
};

const NOOP_RESULT: AutoDistrictResearchResult = {
  checked: 0,
  enqueued: [],
  skipped_fresh: 0,
  skipped_claimed: 0,
  failed: 0,
};

function buildIngestKey(districtId: string, runYear: number): string {
  return `elections:${districtId}:${runYear}`;
}

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function toDraftPayload(district: AddressResolvedDistrict): ElectionDraftPayload {
  return {
    district_id: district.id,
    district_name: district.name,
    district_type: district.district_type,
    state: district.state,
  };
}

/**
 * Demand-driven district research: when an address lookup resolves districts that were
 * never researched (districts.last_elections_searched_at IS NULL) or researched longer
 * than ttlDays ago, enqueue an election draft into the regular pipeline (staging_items
 * upsert + XADD to the elections draft stream). The existing enricher/validator/writer
 * workers take it from there, including candidate roster/profile/record and ballot
 * measure fan-out. The staging_items unique ingest_key upsert doubles as the
 * concurrency claim, so concurrent lookups for the same district enqueue at most once.
 *
 * The trigger never throws: it is meant to be called fire-and-forget from the address
 * API and must never affect the address response.
 */
export function createAutoDistrictResearchTrigger(deps: TriggerDeps): AutoDistrictResearchTrigger {
  const { db, getRedis, config } = deps;
  const now = deps.now ?? (() => new Date());

  return async (districts) => {
    if (!config.enabled || districts.length === 0) {
      return { ...NOOP_RESULT, enqueued: [] };
    }

    try {
      const redis = getRedis();
      if (!redis || !redis.isOpen) {
        // Never claim staging rows we cannot emit to the draft stream.
        console.warn("auto district research skipped: redis unavailable");
        return { ...NOOP_RESULT, checked: districts.length, enqueued: [] };
      }

      const districtIds = districts.map((district) => district.id);
      const staleResult = await db.query(
        `
          SELECT id
          FROM public.districts
          WHERE id = ANY($1::uuid[])
            AND (
              last_elections_searched_at IS NULL
              OR last_elections_searched_at < now() - make_interval(days => $2::int)
            )
        `,
        [districtIds, config.ttlDays]
      );

      const staleIds = new Set(staleResult.rows.map((row: { id: string }) => row.id));
      const staleDistricts = districts.filter((district) => staleIds.has(district.id));
      const result: AutoDistrictResearchResult = {
        checked: districts.length,
        enqueued: [],
        skipped_fresh: districts.length - staleDistricts.length,
        skipped_claimed: 0,
        failed: 0,
      };

      if (staleDistricts.length === 0) {
        return result;
      }

      const env = getPipelineEnv();
      const currentTime = now();
      const runYear = currentTime.getUTCFullYear();
      const runId = `auto_district_research_${currentTime.toISOString()}`;

      for (const district of staleDistricts) {
        const draft = toDraftPayload(district);
        const ingestKey = buildIngestKey(draft.district_id, runYear);
        const serializedPayload = JSON.stringify(draft);

        try {
          const upsert = await db.query(
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
              WHERE staging_items.status IN ('failed', 'rejected', 'written', 'no_results')
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
            ]
          );

          if (upsert.rowCount === 0) {
            result.skipped_claimed += 1;
            continue;
          }

          await redis.xAdd(STAGING_DRAFT_STREAM, "*", {
            ingest_key: ingestKey,
            item_type: STAGING_ITEM_TYPE_ELECTION,
            run_id: runId,
            payload: serializedPayload,
          });
          result.enqueued.push(district.id);
        } catch (error) {
          result.failed += 1;
          console.warn("auto district research enqueue failed:", {
            districtId: district.id,
            ingestKey,
            reason: toReason(error),
          });
        }
      }

      if (result.enqueued.length > 0) {
        console.log("auto district research enqueued districts:", {
          runId,
          enqueued: result.enqueued,
          skipped_fresh: result.skipped_fresh,
          skipped_claimed: result.skipped_claimed,
          failed: result.failed,
        });
      }

      return result;
    } catch (error) {
      console.warn("auto district research trigger failed:", toReason(error));
      return { ...NOOP_RESULT, checked: districts.length, enqueued: [], failed: districts.length };
    }
  };
}
