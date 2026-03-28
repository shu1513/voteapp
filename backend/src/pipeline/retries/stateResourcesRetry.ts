import { Pool } from "pg";
import { createClient } from "redis";

import {
  STAGING_DRAFT_STREAM,
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STAGING_PENDING_STREAM,
} from "../../config/stateResourcePipeline.js";
import {
  STATE_RESOURCE_ABBREVIATION_REGEX,
  STATE_RESOURCE_DRAFT_SCHEMA_VERSION,
  STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
  STATE_RESOURCE_FIPS_REGEX,
  STATE_RESOURCE_SOURCE_FIELDS,
} from "../../contracts/stateResourceEnrichmentContract.js";
import { normalizeRetryFeedback } from "../../ai/retryFeedback.js";
import type { SourceCitation, StateResourceDraftPayload, StateResourcePayload } from "../../types/stateResource.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";
import { normalizeRunId } from "../utils/runIdGuard.js";

type RetryOptions = {
  maxItems?: number;
};

type RetryRow = {
  ingest_key: string;
  status: string;
  reason: string | null;
  run_id: string | null;
  schema_version: string | null;
  payload: unknown;
  failure_debug: unknown;
  ai_raw_debug: unknown;
};

export type RetrySweepResult = {
  scanned: number;
  requeuedToDraft: number;
  requeuedToPending: number;
  skipped: number;
  failed: number;
};

const DUPLICATE_CITATION_FRAGMENT = "contains duplicate citation source_url values";

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function extractHttpUrlsFromText(text: string): string[] {
  const matches = text.match(/https?:\/\/[^\s"'<>)}\]]+/g) ?? [];
  const normalized: string[] = [];
  const seen = new Set<string>();

  for (const match of matches) {
    const cleaned = match.replace(/[.,;:!?]+$/g, "");
    if (cleaned.length === 0 || seen.has(cleaned)) {
      continue;
    }
    seen.add(cleaned);
    normalized.push(cleaned);
  }

  return normalized;
}

function extractFailedCitationUrlsFromFailureDebug(failureDebug: unknown): string[] {
  if (!isObjectRecord(failureDebug)) {
    return [];
  }

  const urls = new Set<string>();
  const failedCitationUrls = failureDebug.failed_citation_urls;
  if (Array.isArray(failedCitationUrls)) {
    for (const candidate of failedCitationUrls) {
      if (!isNonEmptyString(candidate)) {
        continue;
      }
      const normalized = normalizeHttpUrl(candidate);
      urls.add(normalized ?? candidate.trim());
    }
  }

  const addFromText = (text: unknown): void => {
    if (!isNonEmptyString(text)) {
      return;
    }
    for (const url of extractHttpUrlsFromText(text)) {
      urls.add(url);
    }
  };

  addFromText(failureDebug.reason);
  addFromText(failureDebug.error);

  const attempts = failureDebug.attempts;
  if (Array.isArray(attempts)) {
    for (const attempt of attempts) {
      if (!isObjectRecord(attempt)) {
        continue;
      }
      addFromText(attempt.reason);
    }
  }

  return Array.from(urls);
}

function buildRetryFeedbackFromRow(row: RetryRow): Record<string, unknown> {
  const previousFeedback =
    isObjectRecord(row.ai_raw_debug) && "retry_feedback" in row.ai_raw_debug
      ? normalizeRetryFeedback((row.ai_raw_debug as Record<string, unknown>).retry_feedback)
      : null;

  const failedCitationUrls = new Set<string>(previousFeedback?.failedCitationUrls ?? []);
  if (isNonEmptyString(row.reason)) {
    for (const url of extractHttpUrlsFromText(row.reason)) {
      failedCitationUrls.add(url);
    }
  }

  for (const url of extractFailedCitationUrlsFromFailureDebug(row.failure_debug)) {
    failedCitationUrls.add(url);
  }

  return {
    previousFailureReason: isNonEmptyString(row.reason) ? row.reason.trim() : null,
    failedCitationUrls: Array.from(failedCitationUrls).slice(0, 50),
    retryCount: (previousFeedback?.retryCount ?? 0) + 1,
    failedAt: new Date().toISOString(),
  };
}

function mergeAiRawDebugWithRetryFeedback(aiRawDebug: unknown, retryFeedback: Record<string, unknown>): Record<string, unknown> {
  const base = isObjectRecord(aiRawDebug) ? { ...aiRawDebug } : {};
  base.retry_feedback = retryFeedback;
  return base;
}

function isSourceCitation(value: unknown): value is SourceCitation {
  return (
    isObjectRecord(value) &&
    isNonEmptyString(value.source_name) &&
    isNonEmptyString(value.source_url) &&
    isHttpUrl(value.source_url)
  );
}

function parseDraftPayload(value: unknown): StateResourceDraftPayload | null {
  if (!isObjectRecord(value)) {
    return null;
  }
  const input = value as Record<string, unknown>;

  const requiredStrings: Array<keyof StateResourceDraftPayload> = [
    "state_fips",
    "state_abbreviation",
    "state_name",
    "census_source_url",
    "state_abbreviation_reference_url",
  ];

  for (const key of requiredStrings) {
    if (!isNonEmptyString(input[key])) {
      return null;
    }
  }

  const seedSources = input.seed_sources;
  if (!Array.isArray(seedSources) || seedSources.length === 0 || !seedSources.every((item) => isNonEmptyString(item))) {
    return null;
  }

  if (typeof input.allow_open_web_research !== "boolean") {
    return null;
  }

  const state_fips = (input.state_fips as string).trim();
  const state_abbreviation = (input.state_abbreviation as string).trim();
  if (!STATE_RESOURCE_FIPS_REGEX.test(state_fips) || !STATE_RESOURCE_ABBREVIATION_REGEX.test(state_abbreviation)) {
    return null;
  }

  const population_estimate =
    typeof input.population_estimate === "number" || input.population_estimate === null
      ? (input.population_estimate as number | null)
      : null;

  return {
    state_fips,
    state_abbreviation,
    state_name: (input.state_name as string).trim(),
    population_estimate,
    census_source_url: (input.census_source_url as string).trim(),
    state_abbreviation_reference_url: (input.state_abbreviation_reference_url as string).trim(),
    seed_sources: seedSources.map((item) => item.trim()),
    allow_open_web_research: input.allow_open_web_research,
  };
}

function parseEnrichedPayload(value: unknown): StateResourcePayload | null {
  if (!isObjectRecord(value)) {
    return null;
  }
  const input = value as Record<string, unknown>;

  const requiredTextFields: Array<keyof StateResourcePayload> = [
    "state_fips",
    "state_abbreviation",
    "state_name",
    "polling_place_url",
    "voter_registration_url",
    "vote_by_mail_info",
    "polling_hours",
    "id_requirements",
  ];

  for (const key of requiredTextFields) {
    if (!isNonEmptyString(input[key])) {
      return null;
    }
  }

  if (!isObjectRecord(input.sources)) {
    return null;
  }

  const sources = input.sources as Record<string, unknown>;
  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const bucket = sources[key];
    if (!Array.isArray(bucket) || bucket.length === 0) {
      return null;
    }
    if (!bucket.every((citation) => isSourceCitation(citation))) {
      return null;
    }
  }

  return {
    state_fips: (input.state_fips as string).trim(),
    state_abbreviation: (input.state_abbreviation as string).trim(),
    state_name: (input.state_name as string).trim(),
    polling_place_url: (input.polling_place_url as string).trim(),
    voter_registration_url: (input.voter_registration_url as string).trim(),
    vote_by_mail_info: (input.vote_by_mail_info as string).trim(),
    polling_hours: (input.polling_hours as string).trim(),
    id_requirements: (input.id_requirements as string).trim(),
    sources: {
      polling_place_url: sources.polling_place_url as SourceCitation[],
      voter_registration_url: sources.voter_registration_url as SourceCitation[],
      vote_by_mail_info: sources.vote_by_mail_info as SourceCitation[],
      polling_hours: sources.polling_hours as SourceCitation[],
      id_requirements: sources.id_requirements as SourceCitation[],
    },
  };
}

function extractDraftFromAiRawDebug(aiRawDebug: unknown): StateResourceDraftPayload | null {
  if (!isObjectRecord(aiRawDebug)) {
    return null;
  }

  return parseDraftPayload(aiRawDebug.draft_snapshot);
}

function dedupeSources(payload: StateResourcePayload): { payload: StateResourcePayload; changed: boolean } {
  let changed = false;
  const nextSources: StateResourcePayload["sources"] = {
    polling_place_url: [],
    voter_registration_url: [],
    vote_by_mail_info: [],
    polling_hours: [],
    id_requirements: [],
  };

  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const seen = new Set<string>();
    for (const citation of payload.sources[key]) {
      const normalized = normalizeHttpUrl(citation.source_url);
      if (!normalized) {
        continue;
      }
      if (seen.has(normalized)) {
        changed = true;
        continue;
      }
      seen.add(normalized);
      nextSources[key].push({
        source_name: citation.source_name.trim(),
        source_url: normalized,
      });
    }
  }

  return {
    changed,
    payload: {
      ...payload,
      sources: nextSources,
    },
  };
}

function buildRetryRunId(rowRunId: string | null): string {
  return normalizeRunId(rowRunId) ?? `state_resources_retry_${new Date().toISOString()}`;
}

async function loadRetryRows(pool: Pool, maxItems: number): Promise<RetryRow[]> {
  const result = await pool.query<RetryRow>(
    `
      SELECT ingest_key, status, reason, run_id, schema_version, payload, ai_raw_debug
           , failure_debug
      FROM staging_items
      WHERE item_type = $1
        AND status IN ('failed', 'rejected')
      ORDER BY updated_at ASC
      LIMIT $2
    `,
    [STAGING_ITEM_TYPE_STATE_RESOURCES, maxItems]
  );

  return result.rows;
}

async function markRetryFailure(pool: Pool, ingestKey: string, reason: string): Promise<void> {
  await pool.query(
    `
      UPDATE staging_items
      SET status = 'failed',
          reason = $2,
          failure_debug = jsonb_build_object(
            'stage', 'retry_sweeper',
            'error', $2
          ),
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $3
    `,
    [ingestKey, reason, STAGING_ITEM_TYPE_STATE_RESOURCES]
  );
}

async function requeueToDraft(
  pool: Pool,
  redis: ReturnType<typeof createClient>,
  row: RetryRow,
  draft: StateResourceDraftPayload
): Promise<boolean> {
  const runId = buildRetryRunId(row.run_id);
  const retryFeedback = buildRetryFeedbackFromRow(row);
  const nextAiRawDebug = mergeAiRawDebugWithRetryFeedback(row.ai_raw_debug, retryFeedback);

  const transition = await pool.query(
    `
      UPDATE staging_items
      SET payload = $2::jsonb,
          status = 'pending',
          reason = NULL,
          failure_debug = NULL,
          ai_raw_debug = $3::jsonb,
          schema_version = $4,
          run_id = $5,
          validated_at = NULL,
          written_at = NULL,
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $6
        AND status IN ('failed', 'rejected')
    `,
    [
      row.ingest_key,
      JSON.stringify(draft),
      JSON.stringify(nextAiRawDebug),
      STATE_RESOURCE_DRAFT_SCHEMA_VERSION,
      runId,
      STAGING_ITEM_TYPE_STATE_RESOURCES,
    ]
  );

  if (transition.rowCount !== 1) {
    return false;
  }

  try {
    await redis.xAdd(STAGING_DRAFT_STREAM, "*", {
      ingest_key: row.ingest_key,
      item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
      run_id: runId,
      payload: JSON.stringify(draft),
    });
    return true;
  } catch (error) {
    await markRetryFailure(pool, row.ingest_key, `retry enqueue to draft failed: ${toReason(error)}`);
    return false;
  }
}

async function requeueToPending(
  pool: Pool,
  redis: ReturnType<typeof createClient>,
  row: RetryRow,
  payload: StateResourcePayload
): Promise<boolean> {
  const runId = buildRetryRunId(row.run_id);

  const transition = await pool.query(
    `
      UPDATE staging_items
      SET payload = $2::jsonb,
          status = 'pending',
          reason = NULL,
          failure_debug = NULL,
          schema_version = $3,
          run_id = $4,
          validated_at = NULL,
          written_at = NULL,
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $5
        AND status IN ('failed', 'rejected')
    `,
    [
      row.ingest_key,
      JSON.stringify(payload),
      STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
      runId,
      STAGING_ITEM_TYPE_STATE_RESOURCES,
    ]
  );

  if (transition.rowCount !== 1) {
    return false;
  }

  try {
    await redis.xAdd(STAGING_PENDING_STREAM, "*", {
      ingest_key: row.ingest_key,
      item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
      run_id: runId,
    });
    return true;
  } catch (error) {
    await markRetryFailure(pool, row.ingest_key, `retry enqueue to pending failed: ${toReason(error)}`);
    return false;
  }
}

/**
 * Retries failed/rejected state_resources rows:
 * - duplicate-citation rejections are auto-deduped and revalidated
 * - rows with draft snapshots are requeued for fresh AI enrichment
 */
export async function runStateResourcesRetrySweeper(options: RetryOptions = {}): Promise<RetrySweepResult> {
  const { maxItems = 200 } = options;

  const databaseUrl = process.env.DATABASE_URL ?? "postgresql://localhost:5432/voteapp";
  const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";

  const pool = new Pool({ connectionString: databaseUrl });
  const redis = createClient({ url: redisUrl });

  let scanned = 0;
  let requeuedToDraft = 0;
  let requeuedToPending = 0;
  let skipped = 0;
  let failed = 0;

  try {
    await redis.connect();
    const rows = await loadRetryRows(pool, maxItems);
    scanned = rows.length;

    for (const row of rows) {
      try {
        const reason = row.reason ?? "";

        if (
          row.status === "rejected" &&
          row.schema_version === STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION &&
          reason.includes(DUPLICATE_CITATION_FRAGMENT)
        ) {
          const enrichedPayload = parseEnrichedPayload(row.payload);
          if (!enrichedPayload) {
            failed += 1;
            await markRetryFailure(pool, row.ingest_key, "retry dedupe failed: invalid enriched payload");
            continue;
          }

          const deduped = dedupeSources(enrichedPayload);
          if (!deduped.changed) {
            skipped += 1;
            continue;
          }

          const ok = await requeueToPending(pool, redis, row, deduped.payload);
          if (ok) {
            requeuedToPending += 1;
          } else {
            failed += 1;
          }
          continue;
        }

        const draftFromPayload = parseDraftPayload(row.payload);
        const draftFromAiRawDebug = extractDraftFromAiRawDebug(row.ai_raw_debug);
        const draft = draftFromPayload ?? draftFromAiRawDebug;

        if (!draft) {
          skipped += 1;
          continue;
        }

        const ok = await requeueToDraft(pool, redis, row, draft);
        if (ok) {
          requeuedToDraft += 1;
        } else {
          failed += 1;
        }
      } catch (error) {
        failed += 1;
        await markRetryFailure(pool, row.ingest_key, `retry sweep error: ${toReason(error)}`);
      }
    }
  } finally {
    await redis.quit();
    await pool.end();
  }

  return {
    scanned,
    requeuedToDraft,
    requeuedToPending,
    skipped,
    failed,
  };
}
