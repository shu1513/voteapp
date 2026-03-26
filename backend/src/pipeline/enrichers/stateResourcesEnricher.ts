import { Pool } from "pg";
import { createClient } from "redis";

import { buildEnrichmentConfigFromEnv, enrichStateResources } from "../../ai/enrichStateResources.js";
import type { EnrichStateResourcesConfig, EvidenceSnippet } from "../../ai/types.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_DRAFT_STREAM,
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STAGING_PENDING_STREAM,
  STAGING_STATE_RESOURCES_ENRICHER_GROUP,
} from "../../config/stateResourcePipeline.js";
import {
  STATE_RESOURCE_ABBREVIATION_REGEX,
  STATE_RESOURCE_DRAFT_SCHEMA_VERSION,
  STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
  STATE_RESOURCE_FIPS_REGEX,
} from "../../contracts/stateResourceEnrichmentContract.js";
import { collectStateResourceEvidence } from "../evidence/stateResourceEvidenceCollector.js";
import type { StateResourceDraftPayload, StateResourcePayload } from "../../types/stateResource.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";
import { hasRunIdMismatch, normalizeRunId } from "../utils/runIdGuard.js";

type EnricherOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type StagingRow = {
  ingest_key: string;
  run_id: string | null;
  payload: unknown;
  status: string;
  schema_version: string | null;
  prompt_version: string | null;
};

type DraftParseResult =
  | { ok: true; draft: StateResourceDraftPayload }
  | { ok: false; reason: string };

type EnricherOutcome = "enriched" | "failed" | "skipped" | "retry" | "recovered";

type EnrichedStagingPayload = StateResourcePayload & {
  evidence: EvidenceSnippet[];
};

// Evidence crawl + provider call can run for >2 minutes; keep reclaim window above worst-case work.
const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const VOTE_ORG_POLLING_LOCATOR_URL = "https://www.vote.org/polling-place-locator/";
const VOTE_ORG_POLLING_FETCH_TIMEOUT_MS = 10_000;
const VOTE_ORG_RETRY_BACKOFF_INITIAL_MS = 60_000;
const VOTE_ORG_RETRY_BACKOFF_MAX_MS = 15 * 60_000;
let voteOrgPollingMapPromise: Promise<Map<string, string>> | null = null;
let voteOrgLastLoadFailureAt: number | null = null;
let voteOrgRetryBackoffMs = VOTE_ORG_RETRY_BACKOFF_INITIAL_MS;

/**
 * Converts unknown errors into bounded strings for logs and DB reason fields.
 */
function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

/**
 * Checks whether a value is a non-empty trimmed string.
 */
function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

/**
 * Validates that a string is an absolute http(s) URL.
 */
function isHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function normalizeStateKey(stateName: string): string {
  return stateName.trim().toLowerCase().replace(/\s+/g, " ");
}

function normalizeWhitespace(input: string): string {
  return input.replace(/\s+/g, " ").trim();
}

function stripHtml(input: string): string {
  return normalizeWhitespace(
    input
      .replace(/<!--[\s\S]*?-->/g, " ")
      .replace(/<[^>]+>/g, " ")
  );
}

async function loadVoteOrgPollingMap(fetchImpl: typeof fetch = fetch): Promise<Map<string, string>> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), VOTE_ORG_POLLING_FETCH_TIMEOUT_MS);

  try {
    const response = await fetchImpl(VOTE_ORG_POLLING_LOCATOR_URL, {
      headers: { "User-Agent": "voteapp-state-resources-enricher/1.0" },
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`vote.org polling locator fetch failed: ${response.status} ${response.statusText}`);
    }

    const html = await response.text();
    const map = new Map<string, string>();
    const anchorRegex = /<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
    let match: RegExpExecArray | null = anchorRegex.exec(html);

    while (match) {
      const href = normalizeHttpUrl(match[1], { baseUrl: VOTE_ORG_POLLING_LOCATOR_URL });
      if (!href) {
        match = anchorRegex.exec(html);
        continue;
      }

      const linkText = stripHtml(match[2]).toLowerCase();
      const suffix = "polling place locator";
      if (!linkText.endsWith(suffix)) {
        match = anchorRegex.exec(html);
        continue;
      }

      const stateName = normalizeWhitespace(linkText.slice(0, -suffix.length));
      if (stateName.length > 0) {
        map.set(normalizeStateKey(stateName), href);
      }

      match = anchorRegex.exec(html);
    }

    return map;
  } finally {
    clearTimeout(timeout);
  }
}

async function getVoteOrgPollingUrlForState(stateName: string): Promise<string | null> {
  if (!voteOrgPollingMapPromise) {
    if (
      voteOrgLastLoadFailureAt !== null &&
      Date.now() - voteOrgLastLoadFailureAt < voteOrgRetryBackoffMs
    ) {
      return null;
    }

    voteOrgPollingMapPromise = loadVoteOrgPollingMap()
      .then((map) => {
        voteOrgLastLoadFailureAt = null;
        voteOrgRetryBackoffMs = VOTE_ORG_RETRY_BACKOFF_INITIAL_MS;
        return map;
      })
      .catch((error) => {
        voteOrgPollingMapPromise = null;
        voteOrgLastLoadFailureAt = Date.now();
        voteOrgRetryBackoffMs = Math.min(
          voteOrgRetryBackoffMs * 2,
          VOTE_ORG_RETRY_BACKOFF_MAX_MS
        );
        throw error;
      });
  }

  try {
    const map = await voteOrgPollingMapPromise;
    return map.get(normalizeStateKey(stateName)) ?? null;
  } catch (error) {
    console.warn(`enricher vote.org polling map unavailable: ${toReason(error)}`);
    return null;
  }
}

/**
 * Validates and normalizes producer draft payload structure from staging JSON.
 */
function parseDraftPayload(payload: unknown): DraftParseResult {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "draft payload must be an object" };
  }

  const input = payload as Record<string, unknown>;

  if (!isNonEmptyString(input.state_fips)) {
    return { ok: false, reason: "draft.state_fips is required" };
  }
  if (!isNonEmptyString(input.state_abbreviation)) {
    return { ok: false, reason: "draft.state_abbreviation is required" };
  }
  if (!isNonEmptyString(input.state_name)) {
    return { ok: false, reason: "draft.state_name is required" };
  }
  if (!isNonEmptyString(input.census_source_url)) {
    return { ok: false, reason: "draft.census_source_url is required" };
  }
  if (!isNonEmptyString(input.state_abbreviation_reference_url)) {
    return { ok: false, reason: "draft.state_abbreviation_reference_url is required" };
  }

  const seedSources = input.seed_sources;
  if (!Array.isArray(seedSources) || seedSources.length === 0 || !seedSources.every(isNonEmptyString)) {
    return { ok: false, reason: "draft.seed_sources must be a non-empty string array" };
  }

  if (typeof input.allow_open_web_research !== "boolean") {
    return { ok: false, reason: "draft.allow_open_web_research must be boolean" };
  }

  const normalized: StateResourceDraftPayload = {
    state_fips: (input.state_fips as string).trim(),
    state_abbreviation: (input.state_abbreviation as string).trim(),
    state_name: (input.state_name as string).trim(),
    population_estimate:
      typeof input.population_estimate === "number" && Number.isFinite(input.population_estimate)
        ? input.population_estimate
        : null,
    census_source_url: (input.census_source_url as string).trim(),
    state_abbreviation_reference_url: (input.state_abbreviation_reference_url as string).trim(),
    seed_sources: (seedSources as string[]).map((item) => item.trim()),
    allow_open_web_research: input.allow_open_web_research,
  };

  if (!normalized.seed_sources.every((url) => isHttpUrl(url))) {
    return { ok: false, reason: "draft.seed_sources must contain valid http(s) URLs" };
  }

  if (!STATE_RESOURCE_FIPS_REGEX.test(normalized.state_fips)) {
    return { ok: false, reason: "draft.state_fips must be exactly two digits" };
  }

  if (!STATE_RESOURCE_ABBREVIATION_REGEX.test(normalized.state_abbreviation)) {
    return { ok: false, reason: "draft.state_abbreviation must be two uppercase letters" };
  }

  if (!isHttpUrl(normalized.census_source_url)) {
    return { ok: false, reason: "draft.census_source_url must be a valid http(s) URL" };
  }

  if (!isHttpUrl(normalized.state_abbreviation_reference_url)) {
    return { ok: false, reason: "draft.state_abbreviation_reference_url must be a valid http(s) URL" };
  }

  return { ok: true, draft: normalized };
}

/**
 * Ensures the draft consumer group exists.
 */
async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, "0", {
      MKSTREAM: true,
    });
  } catch (error) {
    const message = toReason(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

/**
 * Reclaims stale pending stream entries for at-least-once resiliency.
 */
async function reclaimPendingEntries(
  redis: ReturnType<typeof createClient>,
  consumerName: string,
  batchSize: number
): Promise<Array<{ id: string; message: Record<string, string> }>> {
  const reclaimed: Array<{ id: string; message: Record<string, string> }> = [];
  let cursor = "0-0";

  for (let i = 0; i < RECLAIM_MAX_BATCHES; i += 1) {
    const claim = await redis.xAutoClaim(
      STAGING_DRAFT_STREAM,
      STAGING_STATE_RESOURCES_ENRICHER_GROUP,
      consumerName,
      RECLAIM_MIN_IDLE_MS,
      cursor,
      { COUNT: batchSize }
    );

    cursor = claim.nextId;
    if (!claim.messages || claim.messages.length === 0) {
      break;
    }

    reclaimed.push(
      ...claim.messages
        .filter((entry): entry is NonNullable<typeof entry> => entry !== null)
        .map((entry) => ({ id: entry.id, message: entry.message as Record<string, string> }))
    );
  }

  return reclaimed;
}

/**
 * Loads one staging row by ingest key for state_resources.
 */
async function getStagingRow(pool: Pool, ingestKey: string): Promise<StagingRow | null> {
  const result = await pool.query<StagingRow>(
    `
      SELECT ingest_key, run_id, payload, status, schema_version, prompt_version
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_STATE_RESOURCES]
  );

  return result.rows[0] ?? null;
}

/**
 * Returns current staging status for one ingest key.
 */
async function getStagingStatus(pool: Pool, ingestKey: string): Promise<string | null> {
  const result = await pool.query<{ status: string }>(
    `
      SELECT status
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_STATE_RESOURCES]
  );
  return result.rows[0]?.status ?? null;
}

/**
 * Marks a pending staging row as failed with persistable reason.
 */
async function markFailedPending(
  pool: Pool,
  ingestKey: string,
  reason: string,
  expectedRunId: string | null
): Promise<void> {
  await pool.query(
    `
      UPDATE staging_items
      SET status = 'failed',
          reason = $2,
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $3
        AND status = 'pending'
        AND run_id IS NOT DISTINCT FROM $4
    `,
    [ingestKey, reason, STAGING_ITEM_TYPE_STATE_RESOURCES, expectedRunId]
  );
}

/**
 * Publishes one ingest key to pending stream so validator can process it.
 */
async function publishPending(
  redis: ReturnType<typeof createClient>,
  ingestKey: string,
  runId: string | null
): Promise<void> {
  await redis.xAdd(STAGING_PENDING_STREAM, "*", {
    ingest_key: ingestKey,
    item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
    run_id: runId ?? "",
  });
}

/**
 * Writes enriched payload to staging row when it is still draft + pending.
 */
async function applyEnrichment(
  pool: Pool,
  ingestKey: string,
  payload: EnrichedStagingPayload,
  promptVersion: string,
  provider: string,
  model: string,
  expectedRunId: string | null
): Promise<boolean> {
  const result = await pool.query(
    `
      UPDATE staging_items
      SET payload = $2::jsonb,
          schema_version = $3,
          model = $4,
          prompt_version = $5,
          reason = NULL,
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $6
        AND status = 'pending'
        AND (schema_version = $7 OR schema_version IS NULL)
        AND run_id IS NOT DISTINCT FROM $8
    `,
    [
      ingestKey,
      JSON.stringify(payload),
      STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
      `${provider}:${model}`,
      promptVersion,
      STAGING_ITEM_TYPE_STATE_RESOURCES,
      STATE_RESOURCE_DRAFT_SCHEMA_VERSION,
      expectedRunId,
    ]
  );

  return result.rowCount === 1;
}

/**
 * Processes one draft message through evidence + AI enrichment and routes to pending validation.
 */
async function processMessage(
  pool: Pool,
  redis: ReturnType<typeof createClient>,
  envPromptVersion: string,
  enrichmentConfig: EnrichStateResourcesConfig,
  messageId: string,
  message: Record<string, string>
): Promise<EnricherOutcome> {
  const ingestKey = message.ingest_key;
  const messageRunId = normalizeRunId(message.run_id);
  if (!ingestKey) {
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "skipped";
  }

  const row = await getStagingRow(pool, ingestKey);
  if (!row) {
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "skipped";
  }

  if (hasRunIdMismatch(message.run_id, row.run_id)) {
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "skipped";
  }
  const expectedRunId = messageRunId ?? normalizeRunId(row.run_id);

  if (row.status !== "pending") {
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "skipped";
  }

  // Recovery path: DB already enriched, likely publish/ack failed earlier.
  if (row.schema_version === STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION) {
    try {
      await publishPending(redis, ingestKey, row.run_id);
      await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
      return "recovered";
    } catch {
      return "retry";
    }
  }

  if (row.schema_version && row.schema_version !== STATE_RESOURCE_DRAFT_SCHEMA_VERSION) {
    await markFailedPending(
      pool,
      ingestKey,
      `Unsupported draft schema_version: ${row.schema_version}`,
      expectedRunId
    );
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "failed";
  }

  const draft = parseDraftPayload(row.payload);
  if (!draft.ok) {
    await markFailedPending(pool, ingestKey, draft.reason, expectedRunId);
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "failed";
  }

  const evidence = await collectStateResourceEvidence(draft.draft);
  if (evidence.length === 0) {
    await markFailedPending(pool, ingestKey, "enricher could not collect evidence snippets", expectedRunId);
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "failed";
  }

  const enrichedEvidence = [...evidence];
  const voteOrgPollingUrl = await getVoteOrgPollingUrlForState(draft.draft.state_name);
  if (
    voteOrgPollingUrl &&
    !enrichedEvidence.some((item) => normalizeHttpUrl(item.url) === normalizeHttpUrl(voteOrgPollingUrl))
  ) {
    enrichedEvidence.unshift({
      url: voteOrgPollingUrl,
      title: "Vote.org",
      snippet: `${draft.draft.state_name} polling place locator`,
    });
  }

  const enrichmentResult = await enrichStateResources(
    {
      ingestKey,
      draft: draft.draft,
      evidence: enrichedEvidence,
      promptVersion: row.prompt_version ?? envPromptVersion,
    },
    enrichmentConfig
  );

  if (!enrichmentResult.ok) {
    const reason = `[${enrichmentResult.errorCode}] ${enrichmentResult.reason}`;

    if (enrichmentResult.retryable) {
      // Keep message unacked so it can be reclaimed/retried with backoff.
      console.warn(`enricher retryable failure ingest_key=${ingestKey}: ${reason}`);
      return "retry";
    }

    await markFailedPending(pool, ingestKey, reason, expectedRunId);
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "failed";
  }

  const enrichedPayload: EnrichedStagingPayload = {
    ...enrichmentResult.payload,
    evidence: enrichedEvidence,
  };

  const didUpdate = await applyEnrichment(
    pool,
    ingestKey,
    enrichedPayload,
    enrichmentResult.promptVersion,
    enrichmentResult.provider,
    enrichmentResult.model,
    expectedRunId
  );

  if (!didUpdate) {
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "skipped";
  }

  try {
    await publishPending(redis, ingestKey, row.run_id);
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "enriched";
  } catch {
    // Keep message unacked for XAUTOCLAIM recovery.
    return "retry";
  }
}

/**
 * Runs the real AI enricher worker loop.
 */
export async function runStateResourcesEnricher(options: EnricherOptions = {}): Promise<void> {
  const { once = false, batchSize = 20, blockMs = 5000 } = options;

  const env = getPipelineEnv();
  const enrichmentConfig = buildEnrichmentConfigFromEnv(env);
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  await redis.connect();
  await ensureConsumerGroup(redis);

  const consumerName = `enricher-${process.pid}`;
  let enriched = 0;
  let recovered = 0;
  let failed = 0;
  let skipped = 0;
  let retried = 0;

  const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
    for (const entry of entries) {
      try {
        const outcome = await processMessage(
          pool,
          redis,
          env.PROMPT_VERSION,
          enrichmentConfig,
          entry.id,
          entry.message
        );

        if (outcome === "enriched") {
          enriched += 1;
        } else if (outcome === "recovered") {
          recovered += 1;
        } else if (outcome === "failed") {
          failed += 1;
        } else if (outcome === "retry") {
          retried += 1;
        } else {
          skipped += 1;
        }
      } catch (error) {
        const ingestKey = entry.message.ingest_key;

        if (!ingestKey) {
          try {
            await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, entry.id);
          } catch {
            retried += 1;
          }
          continue;
        }

        const status = await getStagingStatus(pool, ingestKey);
        if (status === "pending") {
          // Unknown error may still be transient; keep unacked for retry/reclaim.
          console.warn(`enricher retrying ingest_key=${ingestKey}: ${toReason(error)}`);
          retried += 1;
          continue;
        }

        try {
          await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, entry.id);
        } catch {
          retried += 1;
        }

        console.error("enricher unexpected error:", toReason(error));
      }
    }
  };

  try {
    let keepRunning = true;

    while (keepRunning) {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_STATE_RESOURCES_ENRICHER_GROUP,
        consumerName,
        [{ key: STAGING_DRAFT_STREAM, id: ">" }],
        { COUNT: batchSize, BLOCK: blockMs }
      );

      if (batches && batches.length > 0) {
        for (const batch of batches) {
          await handleEntries(batch.messages.map((entry) => ({ id: entry.id, message: entry.message })));
        }
      }

      if (once) {
        keepRunning = false;
      }
    }
  } finally {
    await redis.quit();
    await pool.end();
  }

  console.log(
    `state_resources enricher completed. enriched=${enriched} recovered=${recovered} failed=${failed} skipped=${skipped} retried=${retried}`
  );
}
