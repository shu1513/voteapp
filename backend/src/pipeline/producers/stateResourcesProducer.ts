import { Pool } from "pg";
import { createClient } from "redis";

import {
  ALLOW_OPEN_WEB_RESEARCH,
  CENSUS_STATES_API_URL,
  EXPECTED_STATE_RESOURCE_STATE_COUNT,
  STATE_ABBREVIATION_REFERENCE_URL,
  STAGING_DRAFT_STREAM,
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STATE_RESOURCE_SEED_SOURCES,
} from "../../config/stateResourcePipeline.js";
import { getPipelineEnv } from "../../config/env.js";
import { STATE_RESOURCE_DRAFT_SCHEMA_VERSION } from "../../contracts/stateResourceEnrichmentContract.js";
import {
  getStateAbbreviationByFips,
  normalizeFips,
  STATE_ABBR_BY_FIPS,
} from "../../constants/usStates.js";
import type { StateResourceDraftPayload } from "../../types/stateResource.js";

type CensusState = {
  state_name: string;
  state_fips: string;
  population_estimate: number | null;
};

type ProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
};

/**
 * Converts unknown errors into a bounded reason string suitable for persistence.
 */
function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

/**
 * Fetches 50 states + DC from the Census API and normalizes the response.
 */
async function fetchCensusStates(): Promise<CensusState[]> {
  const response = await fetch(CENSUS_STATES_API_URL);

  if (!response.ok) {
    throw new Error(`Census API request failed: ${response.status} ${response.statusText}`);
  }

  const data: unknown = await response.json();

  if (!Array.isArray(data) || data.length < 2) {
    throw new Error("Unexpected Census response format: expected array with header and rows");
  }

  const rows = data.slice(1);
  const states: CensusState[] = [];

  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 3) {
      continue;
    }

    const [nameRaw, populationRaw, fipsRaw] = row;

    if (typeof nameRaw !== "string" || typeof populationRaw !== "string" || typeof fipsRaw !== "string") {
      continue;
    }

    const state_fips = normalizeFips(fipsRaw.trim());

    // Filter out territories by only allowing fips values in our canonical 50+DC map.
    if (!Object.hasOwn(STATE_ABBR_BY_FIPS, state_fips)) {
      continue;
    }

    const parsedPopulation = Number.parseInt(populationRaw, 10);
    states.push({
      state_name: nameRaw.trim(),
      state_fips,
      population_estimate: Number.isFinite(parsedPopulation) ? parsedPopulation : null,
    });
  }

  const distinctFips = new Set(states.map((s) => s.state_fips));
  if (distinctFips.size !== EXPECTED_STATE_RESOURCE_STATE_COUNT) {
    const missing = Object.keys(STATE_ABBR_BY_FIPS)
      .sort()
      .filter((fips) => !distinctFips.has(fips));
    throw new Error(
      `Expected ${EXPECTED_STATE_RESOURCE_STATE_COUNT} states (50 + DC) from Census, got ${distinctFips.size}. Missing FIPS: ${missing.join(", ")}`
    );
  }

  return states;
}

/**
 * Maps normalized Census state data into a draft state_resources staging payload.
 */
function toDraftPayload(state: CensusState): StateResourceDraftPayload {
  return {
    state_fips: state.state_fips,
    state_abbreviation: getStateAbbreviationByFips(state.state_fips),
    state_name: state.state_name,
    population_estimate: state.population_estimate,
    census_source_url: CENSUS_STATES_API_URL,
    state_abbreviation_reference_url: STATE_ABBREVIATION_REFERENCE_URL,
    seed_sources: STATE_RESOURCE_SEED_SOURCES,
    allow_open_web_research: ALLOW_OPEN_WEB_RESEARCH,
  };
}

/**
 * Builds a deterministic per-state ingest key for a given run year.
 */
function buildIngestKey(stateFips: string, runYear: number): string {
  return `state_resources:${stateFips}:${runYear}`;
}

/**
 * Produces draft state_resources staging items and enqueues them to the draft stream.
 */
export async function runStateResourcesProducer(options: ProducerOptions = {}): Promise<void> {
  const { dryRun = false, force = false } = options;
  const env = getPipelineEnv();
  const runYear = new Date().getUTCFullYear();
  const runId = `state_resources_${new Date().toISOString()}`;

  const states = await fetchCensusStates();
  const payloads = states.map(toDraftPayload);

  if (dryRun) {
    console.log(`[DRY RUN] fetched ${payloads.length} state_resources draft items`);
    console.log(payloads.slice(0, 3));
    return;
  }

  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  await redis.connect();

  let enqueued = 0;
  let skipped = 0;
  let failed = 0;

  try {
    for (const payload of payloads) {
      const ingestKey = buildIngestKey(payload.state_fips, runYear);
      const serializedPayload = JSON.stringify(payload);

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
            STAGING_ITEM_TYPE_STATE_RESOURCES,
            serializedPayload,
            runId,
            env.AI_MODEL,
            STATE_RESOURCE_DRAFT_SCHEMA_VERSION,
            env.PROMPT_VERSION,
            force,
          ]
        );

        // Enqueue only when this is a brand-new staging record, retry of failed/rejected data,
        // or when force refresh is enabled.
        if (result.rowCount === 0) {
          skipped += 1;
          continue;
        }

        await redis.xAdd(STAGING_DRAFT_STREAM, "*", {
          ingest_key: ingestKey,
          item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
          run_id: runId,
          payload: serializedPayload,
        });

        enqueued += 1;
      } catch (error) {
        failed += 1;
        const reason = toReason(error);

        await pool.query(
          `
            UPDATE staging_items
            SET status = 'failed',
                reason = $2,
                updated_at = now()
            WHERE ingest_key = $1
          `,
          [ingestKey, reason]
        );
      }
    }
  } finally {
    await redis.quit();
    await pool.end();
  }

  console.log(
    `state_resources producer completed. enqueued=${enqueued} skipped=${skipped} failed=${failed} force=${force}`
  );
}
