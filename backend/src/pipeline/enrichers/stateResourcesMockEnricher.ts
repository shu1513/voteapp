import { Pool } from "pg";
import { createClient } from "redis";

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
  STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH,
  STATE_RESOURCE_REQUIRED_TEXT_FIELDS,
  STATE_RESOURCE_SOURCE_FIELDS,
  STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH,
} from "../../contracts/stateResourceEnrichmentContract.js";
import { collectStateResourceEvidence } from "../evidence/stateResourceEvidenceCollector.js";
import type { EvidenceSnippet } from "../../ai/types.js";
import type { StateResourceDraftPayload, StateResourcePayload, StateResourceSources } from "../../types/stateResource.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";
import { isLikelyPollingPlaceUrl as isLikelyPollingPlaceUrlByUrl } from "../../utils/isLikelyPollingPlaceUrl.js";
import { hasRunIdMismatch, normalizeRunId } from "../utils/runIdGuard.js";

type MockEnricherOptions = {
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

// Evidence crawling can take tens of seconds; keep reclaim window above crawl budget.
const RECLAIM_MIN_IDLE_MS = 180_000;
const RECLAIM_MAX_BATCHES = 20;
const VOTE_ORG_POLLING_LOCATOR_URL = "https://www.vote.org/polling-place-locator/";
const VOTE_ORG_POLLING_FETCH_TIMEOUT_MS = 10_000;
let voteOrgPollingMapPromise: Promise<Map<string, string>> | null = null;

/**
 * Converts unknown errors into bounded strings for logs and DB reasons.
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
 * Validates that a string is an http(s) URL.
 */
function isHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

/**
 * Validates and normalizes draft payload structure from staging JSON.
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

type EnrichedStagingPayload = StateResourcePayload & {
  evidence: EvidenceSnippet[];
};

/**
 * Derives a citation source name from evidence title.
 */
function sourceNameFromEvidence(evidence: EvidenceSnippet): string {
  return evidence.title.trim().length > 0 ? evidence.title.trim() : "source";
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
    voteOrgPollingMapPromise = loadVoteOrgPollingMap().catch((error) => {
      voteOrgPollingMapPromise = null;
      throw error;
    });
  }

  const map = await voteOrgPollingMapPromise;
  return map.get(normalizeStateKey(stateName)) ?? null;
}

const AGGREGATOR_HOSTS = new Set([
  "vote.org",
  "www.vote.org",
  "nass.org",
  "www.nass.org",
  "usvotefoundation.org",
  "www.usvotefoundation.org",
]);

/**
 * Extracts hostname for deterministic URL scoring.
 */
function getHostname(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Returns true when URL/title/snippet contain clear signal for the target state.
 */
function hasStateSignal(url: string, title: string, snippet: string, draft: StateResourceDraftPayload): boolean {
  const lowerUrl = url.toLowerCase();
  const stateNameLower = draft.state_name.trim().toLowerCase();
  const stateSlug = stateNameLower.replace(/\s+/g, "-");
  const stateCompact = stateNameLower.replace(/[^a-z0-9]/g, "");
  const urlCompact = lowerUrl.replace(/[^a-z0-9]/g, "");
  const abbreviationLower = draft.state_abbreviation.trim().toLowerCase();
  const titleLower = title.toLowerCase();
  const snippetLower = snippet.toLowerCase();

  if (
    titleLower.includes(stateNameLower) ||
    snippetLower.includes(stateNameLower) ||
    lowerUrl.includes(`/${stateSlug}`) ||
    (stateCompact.length > 3 && urlCompact.includes(stateCompact))
  ) {
    return true;
  }

  if (abbreviationLower.length === 2) {
    if (lowerUrl.includes(`.${abbreviationLower}.`) || lowerUrl.includes(`/${abbreviationLower}/`)) {
      return true;
    }
  }

  return false;
}

/**
 * Scores candidate polling URLs so official, state-specific links beat aggregators.
 */
function scorePollingEvidence(item: EvidenceSnippet, draft: StateResourceDraftPayload): number {
  const normalizedUrl = normalizeHttpUrl(item.url);
  if (!normalizedUrl) {
    return Number.NEGATIVE_INFINITY;
  }

  const host = getHostname(normalizedUrl);
  const urlLower = normalizedUrl.toLowerCase();
  const titleLower = item.title.toLowerCase();
  const snippetLower = item.snippet.toLowerCase();
  const combined = `${titleLower} ${snippetLower}`;
  const stateNameLower = draft.state_name.trim().toLowerCase();
  const stateSlug = stateNameLower.replace(/\s+/g, "-");
  const stateAbbreviationLower = draft.state_abbreviation.trim().toLowerCase();
  const hasPollingSignal =
    isLikelyPollingPlaceUrlByUrl(urlLower) || /\b(polling|locator|find your polling place)\b/.test(combined);
  const hasOnlyNonPollingSignal =
    /\b(register|registration|absentee|mail|id[-\s]?laws?|identification)\b/.test(combined) &&
    !hasPollingSignal;
  const stateSignal = hasStateSignal(normalizedUrl, item.title, item.snippet, draft);

  let score = 0;

  if (hasPollingSignal) {
    score += 40;
  }
  if (host.endsWith(".gov")) {
    score += 40;
  }
  if (combined.includes(stateNameLower) && /\b(polling|locator)\b/.test(combined)) {
    score += 30;
  }
  if (urlLower.includes(`/${stateSlug}`)) {
    score += 25;
  }
  if (urlLower.includes(`.${stateAbbreviationLower}.`)) {
    score += 20;
  }
  if (/(sos\.|secretary-?of-?state|elections?)/.test(`${host}${urlLower}`)) {
    score += 20;
  }
  if (AGGREGATOR_HOSTS.has(host)) {
    score -= 35;
  }
  if (hasOnlyNonPollingSignal) {
    score -= 30;
  }
  if (stateSignal) {
    score += 120;
  } else if (!AGGREGATOR_HOSTS.has(host)) {
    // Avoid selecting another state's official page for this state.
    score -= 220;
  }

  return score;
}

/**
 * Picks the best evidence entry by URL pattern, with deterministic fallback.
 */
function pickEvidenceUrl(
  evidence: EvidenceSnippet[],
  preferredPatterns: RegExp[]
): EvidenceSnippet | null {
  for (const item of evidence) {
    for (const pattern of preferredPatterns) {
      if (pattern.test(item.url)) {
        return item;
      }
    }
  }

  return evidence[0] ?? null;
}

/**
 * Picks the best polling-place evidence, preferring official state-specific URLs.
 */
function pickBestPollingPlaceEvidence(
  evidence: EvidenceSnippet[],
  draft: StateResourceDraftPayload
): EvidenceSnippet | null {
  const scored = evidence
    .map((item) => ({ item, score: scorePollingEvidence(item, draft) }))
    .filter((entry) => Number.isFinite(entry.score) && entry.score > 0)
    .sort((a, b) => b.score - a.score);

  if (scored.length > 0) {
    return scored[0].item;
  }

  return pickEvidenceUrl(evidence, [/polling-place/i, /find-your-polling-place/i]);
}

/**
 * Builds deterministic mock enriched payload from draft + evidence.
 */
function buildMockPayload(draft: StateResourceDraftPayload, evidence: EvidenceSnippet[]): StateResourcePayload | null {
  let pollingPlaceEvidence = pickBestPollingPlaceEvidence(evidence, draft);
  const registrationEvidence = pickEvidenceUrl(
    evidence,
    [/register/i, /voter-registration/i]
  );
  const voteByMailEvidence = pickEvidenceUrl(
    evidence,
    [/absentee/i, /mail/i]
  );
  const pollingHoursEvidence = pickEvidenceUrl(
    evidence,
    [/polling[-_]?hours/i, /can-i-vote/i, /\bhours\b/i]
  );
  const idRequirementsEvidence = pickEvidenceUrl(
    evidence,
    [/\bvoter[-\s]?id\b/i, /\bid[-\s]?requirements?\b/i, /\bidentification\b/i, /voter-id-laws/i]
  );

  if (
    !pollingPlaceEvidence ||
    !registrationEvidence ||
    !voteByMailEvidence ||
    !pollingHoursEvidence ||
    !idRequirementsEvidence
  ) {
    return null;
  }

  const voteByMailInfo = `${draft.state_name} voters can request and return vote-by-mail ballots based on state deadlines and local election rules.`;
  const pollingHours = "Polling locations usually open and close at posted local hours on election day.";
  const idRequirements = `${draft.state_name} voter ID requirements depend on election type and local/state rules.`;

  const sources: StateResourceSources = {
    polling_place_url: [
      { source_name: sourceNameFromEvidence(pollingPlaceEvidence), source_url: pollingPlaceEvidence.url },
    ],
    voter_registration_url: [
      { source_name: sourceNameFromEvidence(registrationEvidence), source_url: registrationEvidence.url },
    ],
    vote_by_mail_info: [
      { source_name: sourceNameFromEvidence(voteByMailEvidence), source_url: voteByMailEvidence.url },
    ],
    polling_hours: [
      { source_name: sourceNameFromEvidence(pollingHoursEvidence), source_url: pollingHoursEvidence.url },
    ],
    id_requirements: [
      { source_name: sourceNameFromEvidence(idRequirementsEvidence), source_url: idRequirementsEvidence.url },
    ],
  };

  return {
    state_fips: draft.state_fips,
    state_abbreviation: draft.state_abbreviation,
    state_name: draft.state_name,
    polling_place_url: pollingPlaceEvidence.url,
    voter_registration_url: registrationEvidence.url,
    vote_by_mail_info: voteByMailInfo,
    polling_hours: pollingHours,
    id_requirements: idRequirements,
    sources,
  };
}

/**
 * Applies hard contract checks to the mock payload before persistence.
 */
function validateMockPayload(payload: StateResourcePayload, evidence: EvidenceSnippet[]): string | null {
  if (!Array.isArray(evidence) || evidence.length === 0) {
    return "mock enricher must collect at least one evidence snippet";
  }

  const evidenceUrlSet = new Set(
    evidence
      .map((item) => normalizeHttpUrl(item.url))
      .filter((url): url is string => typeof url === "string")
  );
  if (evidenceUrlSet.size === 0) {
    return "mock enricher evidence must contain valid http(s) URLs";
  }

  for (const key of STATE_RESOURCE_REQUIRED_TEXT_FIELDS) {
    if (!isNonEmptyString(payload[key])) {
      return `mock payload missing required field: ${key}`;
    }
  }

  if (!STATE_RESOURCE_FIPS_REGEX.test(payload.state_fips)) {
    return "mock payload state_fips must be exactly two digits";
  }
  if (!STATE_RESOURCE_ABBREVIATION_REGEX.test(payload.state_abbreviation)) {
    return "mock payload state_abbreviation must be two uppercase letters";
  }
  if (!isHttpUrl(payload.polling_place_url)) {
    return "mock payload polling_place_url must be a valid http(s) URL";
  }
  if (!isHttpUrl(payload.voter_registration_url)) {
    return "mock payload voter_registration_url must be a valid http(s) URL";
  }
  if (payload.vote_by_mail_info.length > STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH) {
    return `mock payload vote_by_mail_info must be ${STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH} chars or fewer`;
  }
  if (payload.polling_hours.length > STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH) {
    return `mock payload polling_hours must be ${STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH} chars or fewer`;
  }

  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const bucket = payload.sources[key];
    if (!Array.isArray(bucket) || bucket.length === 0) {
      return `mock payload sources.${key} must be a non-empty array`;
    }

    for (const citation of bucket) {
      if (!isNonEmptyString(citation.source_name) || !isNonEmptyString(citation.source_url)) {
        return `mock payload sources.${key} citations require source_name + source_url`;
      }
      const normalizedCitationUrl = normalizeHttpUrl(citation.source_url.trim());
      if (!normalizedCitationUrl) {
        return `mock payload sources.${key}.source_url must be valid http(s)`;
      }
      if (!evidenceUrlSet.has(normalizedCitationUrl)) {
        return `mock payload sources.${key}.source_url must come from collected evidence URLs`;
      }
    }
  }

  return null;
}

/**
 * Ensures the enricher stream consumer group exists.
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
 * Reclaims stale pending stream entries for at-least-once resilience.
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
 * Loads a staging row by ingest key for state_resources.
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
 * Returns current staging status for a given ingest key.
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
 * Marks a pending staging row as failed with reason.
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
 * Publishes an item to the pending stream for validator processing.
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
 * Writes enriched payload into staging row if it is still draft + pending.
 */
async function applyMockEnrichment(
  pool: Pool,
  ingestKey: string,
  payload: EnrichedStagingPayload,
  fallbackPromptVersion: string,
  expectedRunId: string | null
): Promise<boolean> {
  const result = await pool.query(
    `
      UPDATE staging_items
      SET payload = $2::jsonb,
          schema_version = $3,
          model = $4,
          prompt_version = COALESCE(prompt_version, $5),
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
      "mock:state_resources_enricher",
      fallbackPromptVersion,
      STAGING_ITEM_TYPE_STATE_RESOURCES,
      STATE_RESOURCE_DRAFT_SCHEMA_VERSION,
      expectedRunId,
    ]
  );

  return result.rowCount === 1;
}

/**
 * Processes one draft stream message through enrichment flow.
 */
async function processMessage(
  pool: Pool,
  redis: ReturnType<typeof createClient>,
  envPromptVersion: string,
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

  // Recovery path: DB is already enriched but previous publish/ack likely failed.
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
    await markFailedPending(pool, ingestKey, "mock enricher could not collect evidence snippets", expectedRunId);
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "failed";
  }

  const enrichedEvidence = [...evidence];
  if (draft.draft.allow_open_web_research) {
    try {
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
    } catch (error) {
      console.warn(`mock enricher vote.org polling map unavailable: ${toReason(error)}`);
    }
  }

  const mockPayload = buildMockPayload(draft.draft, enrichedEvidence);
  if (!mockPayload) {
    await markFailedPending(
      pool,
      ingestKey,
      "mock enricher could not map evidence to required fields",
      expectedRunId
    );
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "failed";
  }

  const validationReason = validateMockPayload(mockPayload, enrichedEvidence);
  if (validationReason) {
    await markFailedPending(pool, ingestKey, validationReason, expectedRunId);
    await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, messageId);
    return "failed";
  }

  const enrichedPayload: EnrichedStagingPayload = {
    ...mockPayload,
    evidence: enrichedEvidence,
  };

  const didUpdate = await applyMockEnrichment(
    pool,
    ingestKey,
    enrichedPayload,
    row.prompt_version ?? envPromptVersion,
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
    // Keep unacked for XAUTOCLAIM recovery.
    return "retry";
  }
}

/**
 * Runs the mock enricher worker loop.
 */
export async function runStateResourcesMockEnricher(options: MockEnricherOptions = {}): Promise<void> {
  const { once = false, batchSize = 20, blockMs = 5000 } = options;

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  await redis.connect();
  await ensureConsumerGroup(redis);

  const consumerName = `mock-enricher-${process.pid}`;
  let enriched = 0;
  let recovered = 0;
  let failed = 0;
  let skipped = 0;
  let retried = 0;

  const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
    for (const entry of entries) {
      try {
        const outcome = await processMessage(pool, redis, env.PROMPT_VERSION, entry.id, entry.message);
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
          console.warn(`mock enricher retrying ingest_key=${ingestKey}: ${toReason(error)}`);
          retried += 1;
          continue;
        }

        try {
          await redis.xAck(STAGING_DRAFT_STREAM, STAGING_STATE_RESOURCES_ENRICHER_GROUP, entry.id);
        } catch {
          retried += 1;
        }

        console.error("mock enricher unexpected error:", toReason(error));
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
    `state_resources mock enricher completed. enriched=${enriched} recovered=${recovered} failed=${failed} skipped=${skipped} retried=${retried}`
  );
}
