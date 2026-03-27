import { Pool } from "pg";
import { createClient } from "redis";

import {
  STATE_RESOURCE_ABBREVIATION_REGEX,
  STATE_RESOURCE_DRAFT_MARKER_FIELDS,
  STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
  STATE_RESOURCE_FIPS_REGEX,
  STATE_RESOURCE_ID_REQUIREMENTS_MAX_LENGTH,
  STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH,
  STATE_RESOURCE_REQUIRED_TEXT_FIELDS,
  STATE_RESOURCE_SOURCE_FIELDS,
  STATE_RESOURCE_TEXT_MIN_LENGTH,
  STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH,
} from "../../contracts/stateResourceEnrichmentContract.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STAGING_PENDING_STREAM,
  STAGING_REJECTED_STREAM,
  STAGING_STATE_RESOURCES_VALIDATOR_GROUP,
  STAGING_VALIDATED_STREAM,
} from "../../config/stateResourcePipeline.js";
import { CURATED_STATE_POLLING_URL_BY_FIPS } from "../../constants/curatedPollingUrls.js";
import { getStateAbbreviationByFips } from "../../constants/usStates.js";
import type {
  SourceCitation,
  StateResourcePayload,
  StateResourceSources,
} from "../../types/stateResource.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";
import { isUrlOnlyText } from "../../utils/isUrlOnlyText.js";
import { isLikelyPollingPlaceUrl } from "../../utils/isLikelyPollingPlaceUrl.js";
import { hasRunIdMismatch, normalizeRunId } from "../utils/runIdGuard.js";

type ValidatorOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type ValidationResult =
  | {
      ok: true;
      payload: StateResourcePayload;
      warnings: string[];
    }
  | {
      ok: false;
      reasons: string[];
    };

type StagingRow = {
  ingest_key: string;
  run_id: string | null;
  schema_version: string | null;
  prompt_version: string | null;
  reason: string | null;
  payload: unknown;
  status: string;
};

const RECLAIM_MIN_IDLE_MS = 30_000;
const RECLAIM_MAX_BATCHES = 20;
/**
 * Converts an unknown error into a bounded, persistable string.
 */
function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

/**
 * Returns true when the input is a non-empty string after trimming.
 */
function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

/**
 * Checks whether a string is an absolute HTTP/HTTPS URL.
 */
function isHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function isCuratedPollingUrlForState(stateFips: string, pollingPlaceUrl: string): boolean {
  const curated = CURATED_STATE_POLLING_URL_BY_FIPS[stateFips];
  if (!curated) {
    return false;
  }

  const normalizedCurated = normalizeHttpUrl(curated);
  const normalizedUrl = normalizeHttpUrl(pollingPlaceUrl);
  return typeof normalizedCurated === "string" && normalizedCurated === normalizedUrl;
}

type EvidenceSnippet = {
  url: string;
  title: string;
  snippet: string;
};

function normalizeSpace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function extractEvidenceSnippets(input: Record<string, unknown>): EvidenceSnippet[] {
  const raw = input.evidence;
  if (!Array.isArray(raw)) {
    return [];
  }

  const snippets: EvidenceSnippet[] = [];
  for (const entry of raw) {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      continue;
    }

    const item = entry as Record<string, unknown>;
    if (!isNonEmptyString(item.url) || !isNonEmptyString(item.snippet)) {
      continue;
    }

    const normalizedUrl = normalizeHttpUrl(item.url.trim());
    if (!normalizedUrl) {
      continue;
    }

    snippets.push({
      url: normalizedUrl,
      title: isNonEmptyString(item.title) ? normalizeSpace(item.title) : "",
      snippet: normalizeSpace(item.snippet),
    });
  }

  return snippets;
}

function getEvidenceBySourceForField(
  citations: SourceCitation[],
  evidence: EvidenceSnippet[]
): Map<string, EvidenceSnippet[]> {
  const citationUrls = new Set<string>();
  for (const citation of citations) {
    const normalized = normalizeHttpUrl(citation.source_url);
    if (normalized) {
      citationUrls.add(normalized);
    }
  }

  const grouped = new Map<string, EvidenceSnippet[]>();
  for (const snippet of evidence) {
    if (!citationUrls.has(snippet.url)) {
      continue;
    }
    const existing = grouped.get(snippet.url) ?? [];
    existing.push(snippet);
    grouped.set(snippet.url, existing);
  }

  return grouped;
}

type IdRequirementStance = "required" | "not_required" | "unknown";

function deriveIdRequirementStance(snippets: EvidenceSnippet[]): IdRequirementStance {
  let hasRequiredSignal = false;
  let hasNotRequiredSignal = false;

  for (const snippet of snippets) {
    const text = `${snippet.title}. ${snippet.snippet}`.toLowerCase();
    const sentences = text.split(/[.!?]+/g);

    for (const rawSentence of sentences) {
      const sentence = normalizeSpace(rawSentence);
      if (!sentence) {
        continue;
      }

      if (
        /\bid\s+is\s+not\s+required\b/.test(sentence) ||
        /\bno\s+(photo\s+)?id\s+(is\s+)?required\b/.test(sentence) ||
        /\bdo\s+not\s+need\b[^.]{0,40}\bid\b/.test(sentence)
      ) {
        hasNotRequiredSignal = true;
        continue;
      }

      if (
        /\b(photo\s+)?id\s+(is\s+)?required\b/.test(sentence) ||
        /\bmust\s+(show|present|provide)\b[^.]{0,40}\bid\b/.test(sentence)
      ) {
        hasRequiredSignal = true;
      }
    }
  }

  if (hasRequiredSignal && !hasNotRequiredSignal) {
    return "required";
  }
  if (hasNotRequiredSignal && !hasRequiredSignal) {
    return "not_required";
  }

  return "unknown";
}

function detectIdRequirementsConflict(snippetsBySource: Map<string, EvidenceSnippet[]>): boolean {
  if (snippetsBySource.size < 2) {
    return false;
  }

  let hasRequired = false;
  let hasNotRequired = false;

  for (const snippets of snippetsBySource.values()) {
    const stance = deriveIdRequirementStance(snippets);
    if (stance === "required") {
      hasRequired = true;
    } else if (stance === "not_required") {
      hasNotRequired = true;
    }
  }

  return hasRequired && hasNotRequired;
}

type VoteByMailFacts = {
  requestDeadlineDays: Set<number>;
  requiresReceivedByElectionDay: boolean;
  allowsPostmarkByElectionDay: boolean;
};

function extractVoteByMailFacts(snippets: EvidenceSnippet[]): VoteByMailFacts {
  const facts: VoteByMailFacts = {
    requestDeadlineDays: new Set<number>(),
    requiresReceivedByElectionDay: false,
    allowsPostmarkByElectionDay: false,
  };

  for (const snippet of snippets) {
    const text = `${snippet.title}. ${snippet.snippet}`.toLowerCase();

    const requestMatches = text.matchAll(/\b(?:request|apply)[^.]{0,80}\b(\d{1,2})\s+days?\s+before\b/g);
    for (const match of requestMatches) {
      const days = Number.parseInt(match[1] ?? "", 10);
      if (Number.isFinite(days) && days >= 0 && days <= 90) {
        facts.requestDeadlineDays.add(days);
      }
    }

    if (/\b(?:must|needs?)\s+be\s+received\b[^.]{0,60}\b(?:by|on)\s+election day\b/.test(text)) {
      facts.requiresReceivedByElectionDay = true;
    }

    if (
      /\bpostmark(?:ed)?\b[^.]{0,60}\b(?:by|on)\s+election day\b/.test(text) &&
      !/\bpostmark(?:ed)?\b[^.]{0,30}\bnot\b/.test(text)
    ) {
      facts.allowsPostmarkByElectionDay = true;
    }
  }

  return facts;
}

function detectVoteByMailConflict(snippetsBySource: Map<string, EvidenceSnippet[]>): boolean {
  if (snippetsBySource.size < 2) {
    return false;
  }

  const factSets: VoteByMailFacts[] = [];
  for (const snippets of snippetsBySource.values()) {
    factSets.push(extractVoteByMailFacts(snippets));
  }

  let hasReceivedByElectionDay = false;
  let hasPostmarkByElectionDay = false;
  const singletonRequestDeadlines = new Set<number>();
  let singletonRequestDeadlineSources = 0;

  for (const facts of factSets) {
    if (facts.requiresReceivedByElectionDay) {
      hasReceivedByElectionDay = true;
    }
    if (facts.allowsPostmarkByElectionDay) {
      hasPostmarkByElectionDay = true;
    }

    // Only compare sources that declare one unambiguous request deadline.
    // If a source lists multiple channels/deadlines, treat it as ambiguous and skip.
    if (facts.requestDeadlineDays.size === 1) {
      singletonRequestDeadlineSources += 1;
      for (const days of facts.requestDeadlineDays) {
        singletonRequestDeadlines.add(days);
      }
    }
  }

  if (hasReceivedByElectionDay && hasPostmarkByElectionDay) {
    return true;
  }

  if (singletonRequestDeadlineSources >= 2 && singletonRequestDeadlines.size > 1) {
    return true;
  }

  return false;
}

function parseTwelveHourToMinutes(token: string): number | null {
  const match = token.trim().match(/^(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?)$/i);
  if (!match) {
    return null;
  }

  let hours = Number.parseInt(match[1] ?? "", 10);
  const minutes = Number.parseInt(match[2] ?? "0", 10);
  const meridiem = (match[3] ?? "").toLowerCase().replace(/\./g, "");

  if (!Number.isFinite(hours) || !Number.isFinite(minutes) || hours < 1 || hours > 12 || minutes < 0 || minutes > 59) {
    return null;
  }

  if (hours === 12) {
    hours = 0;
  }
  if (meridiem.startsWith("p")) {
    hours += 12;
  }

  return hours * 60 + minutes;
}

type PollingHoursFacts = {
  hasVarySignal: boolean;
  ranges: Set<string>;
};

function extractPollingHoursFacts(snippets: EvidenceSnippet[]): PollingHoursFacts {
  const facts: PollingHoursFacts = {
    hasVarySignal: false,
    ranges: new Set<string>(),
  };

  const rangeRegex =
    /\b(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\s*(?:-|to|until)\s*(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))\b/gi;

  for (const snippet of snippets) {
    const lower = `${snippet.title} ${snippet.snippet}`.toLowerCase();
    if (/var(y|ies)\s+by\s+(county|precinct|location)/.test(lower)) {
      facts.hasVarySignal = true;
    }

    const text = `${snippet.title}. ${snippet.snippet}`;
    for (const match of text.matchAll(rangeRegex)) {
      const start = parseTwelveHourToMinutes(match[1] ?? "");
      const end = parseTwelveHourToMinutes(match[2] ?? "");
      if (start === null || end === null || start === end) {
        continue;
      }
      facts.ranges.add(`${start}-${end}`);
    }
  }

  return facts;
}

function detectPollingHoursConflict(snippetsBySource: Map<string, EvidenceSnippet[]>): boolean {
  if (snippetsBySource.size < 2) {
    return false;
  }

  const signatures = new Set<string>();

  for (const snippets of snippetsBySource.values()) {
    const facts = extractPollingHoursFacts(snippets);
    if (facts.hasVarySignal) {
      return false;
    }
    if (facts.ranges.size === 0) {
      continue;
    }
    const signature = Array.from(facts.ranges).sort().join("|");
    if (signature.length > 0) {
      signatures.add(signature);
    }
  }

  return signatures.size > 1;
}

function detectConflictWarnings(payload: StateResourcePayload, evidence: EvidenceSnippet[]): string[] {
  if (evidence.length === 0) {
    return [];
  }

  const warnings: string[] = [];

  const idBySource = getEvidenceBySourceForField(payload.sources.id_requirements, evidence);
  if (idBySource.size > 1 && detectIdRequirementsConflict(idBySource)) {
    warnings.push("id_requirements citations show conflicting required vs not-required ID signals");
  }

  const voteByMailBySource = getEvidenceBySourceForField(payload.sources.vote_by_mail_info, evidence);
  if (voteByMailBySource.size > 1 && detectVoteByMailConflict(voteByMailBySource)) {
    warnings.push("vote_by_mail_info citations show conflicting deadline/rule statements");
  }

  const pollingHoursBySource = getEvidenceBySourceForField(payload.sources.polling_hours, evidence);
  if (pollingHoursBySource.size > 1 && detectPollingHoursConflict(pollingHoursBySource)) {
    warnings.push("polling_hours citations show conflicting polling-time signals");
  }

  return warnings;
}

/**
 * Validates one citation object inside a sources array.
 */
function validateSourceCitation(value: unknown): value is SourceCitation {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const item = value as Record<string, unknown>;

  if (!isNonEmptyString(item.source_name)) {
    return false;
  }

  if (!isNonEmptyString(item.source_url)) {
    return false;
  }

  return isHttpUrl(item.source_url);
}

/**
 * Validates the sources object required for state_resources records.
 */
function validateSources(value: unknown): { ok: true; sources: StateResourceSources } | { ok: false; reason: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ok: false, reason: "sources must be an object" };
  }

  const obj = value as Record<string, unknown>;

  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const citations = obj[key];

    if (!Array.isArray(citations) || citations.length === 0) {
      return { ok: false, reason: `sources.${key} must be a non-empty array` };
    }

    const seenNormalizedUrls = new Set<string>();
    for (const citation of citations) {
      if (!validateSourceCitation(citation)) {
        return {
          ok: false,
          reason: `sources.${key} contains an invalid citation (requires source_name + http(s) source_url)`,
        };
      }

      const normalizedCitationUrl = normalizeHttpUrl(citation.source_url);
      if (!normalizedCitationUrl) {
        return {
          ok: false,
          reason: `sources.${key} contains an invalid citation URL after normalization`,
        };
      }

      if (seenNormalizedUrls.has(normalizedCitationUrl)) {
        return {
          ok: false,
          reason: `sources.${key} contains duplicate citation source_url values`,
        };
      }
      seenNormalizedUrls.add(normalizedCitationUrl);
    }
  }

  const allowedKeys = new Set(STATE_RESOURCE_SOURCE_FIELDS);
  const extraKeys = Object.keys(obj).filter((key) => !allowedKeys.has(key as keyof StateResourceSources));
  if (extraKeys.length > 0) {
    return { ok: false, reason: `sources contains unsupported keys: ${extraKeys.join(", ")}` };
  }

  return { ok: true, sources: obj as StateResourceSources };
}

/**
 * Validates a staging payload as a complete state_resources candidate.
 */
function validateStateResourcePayload(payload: unknown): ValidationResult {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reasons: ["payload must be an object"] };
  }

  const input = payload as Record<string, unknown>;
  const reasons: string[] = [];
  const looksLikeDraftPayload = STATE_RESOURCE_DRAFT_MARKER_FIELDS.some((key) => Object.hasOwn(input, key));

  for (const key of STATE_RESOURCE_REQUIRED_TEXT_FIELDS) {
    if (!isNonEmptyString(input[key])) {
      reasons.push(`${key} is required and must be a non-empty string`);
    }
  }

  if (reasons.length > 0) {
    if (looksLikeDraftPayload) {
      reasons.unshift("payload is a census draft and has not been AI-enriched with required state_resources fields yet");
    }
    return { ok: false, reasons };
  }

  const state_fips = (input.state_fips as string).trim();
  const state_abbreviation = (input.state_abbreviation as string).trim();
  const state_name = (input.state_name as string).trim();
  const polling_place_url = (input.polling_place_url as string).trim();
  const voter_registration_url = (input.voter_registration_url as string).trim();
  const vote_by_mail_info = (input.vote_by_mail_info as string).trim();
  const polling_hours = (input.polling_hours as string).trim();
  const id_requirements = (input.id_requirements as string).trim();

  if (!STATE_RESOURCE_FIPS_REGEX.test(state_fips)) {
    reasons.push("state_fips must be exactly two digits");
  }

  if (!STATE_RESOURCE_ABBREVIATION_REGEX.test(state_abbreviation)) {
    reasons.push("state_abbreviation must be two uppercase letters");
  }

  if (!isHttpUrl(polling_place_url)) {
    reasons.push("polling_place_url must be a valid http(s) URL");
  }
  if (
    isHttpUrl(polling_place_url) &&
    !isLikelyPollingPlaceUrl(polling_place_url) &&
    !isCuratedPollingUrlForState(state_fips, polling_place_url)
  ) {
    reasons.push("polling_place_url must be a polling-place locator URL, not a registration/mail/id URL");
  }

  if (!isHttpUrl(voter_registration_url)) {
    reasons.push("voter_registration_url must be a valid http(s) URL");
  }

  if (vote_by_mail_info.length > STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH) {
    reasons.push(`vote_by_mail_info must be ${STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH} characters or fewer`);
  }

  if (polling_hours.length > STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH) {
    reasons.push(`polling_hours must be ${STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH} characters or fewer`);
  }

  if (id_requirements.length > STATE_RESOURCE_ID_REQUIREMENTS_MAX_LENGTH) {
    reasons.push(`id_requirements must be ${STATE_RESOURCE_ID_REQUIREMENTS_MAX_LENGTH} characters or fewer`);
  }

  if (isUrlOnlyText(vote_by_mail_info)) {
    reasons.push("vote_by_mail_info must be plain-language text, not a URL");
  }

  if (isUrlOnlyText(polling_hours)) {
    reasons.push("polling_hours must be plain-language text, not a URL");
  }

  if (isUrlOnlyText(id_requirements)) {
    reasons.push("id_requirements must be plain-language text, not a URL");
  }

  if (vote_by_mail_info.length < STATE_RESOURCE_TEXT_MIN_LENGTH) {
    reasons.push(`vote_by_mail_info must be at least ${STATE_RESOURCE_TEXT_MIN_LENGTH} characters`);
  }

  if (polling_hours.length < STATE_RESOURCE_TEXT_MIN_LENGTH) {
    reasons.push(`polling_hours must be at least ${STATE_RESOURCE_TEXT_MIN_LENGTH} characters`);
  }

  if (id_requirements.length < STATE_RESOURCE_TEXT_MIN_LENGTH) {
    reasons.push(`id_requirements must be at least ${STATE_RESOURCE_TEXT_MIN_LENGTH} characters`);
  }

  try {
    const expectedAbbreviation = getStateAbbreviationByFips(state_fips);
    if (state_abbreviation !== expectedAbbreviation) {
      reasons.push(
        `state_abbreviation does not match deterministic mapping for fips ${state_fips} (expected ${expectedAbbreviation})`
      );
    }
  } catch (error) {
    reasons.push(toReason(error));
  }

  const sourcesResult = validateSources(input.sources);
  if (!sourcesResult.ok) {
    reasons.push(sourcesResult.reason);
  }

  if (reasons.length > 0 || !sourcesResult.ok) {
    return { ok: false, reasons };
  }

  return {
    ok: true,
    payload: {
      state_fips,
      state_abbreviation,
      state_name,
      polling_place_url,
      voter_registration_url,
      vote_by_mail_info,
      polling_hours,
      id_requirements,
      sources: sourcesResult.sources,
    },
    warnings: detectConflictWarnings(
      {
        state_fips,
        state_abbreviation,
        state_name,
        polling_place_url,
        voter_registration_url,
        vote_by_mail_info,
        polling_hours,
        id_requirements,
        sources: sourcesResult.sources,
      },
      extractEvidenceSnippets(input)
    ),
  };
}

/**
 * Validates required staging metadata for enriched state_resources payloads.
 */
function validateStagingMetadata(row: StagingRow): string[] {
  const reasons: string[] = [];

  if (!isNonEmptyString(row.schema_version)) {
    reasons.push("schema_version metadata is required");
  } else if (row.schema_version !== STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION) {
    reasons.push(
      `schema_version must be ${STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION} for validator input (got ${row.schema_version})`
    );
  }

  if (!isNonEmptyString(row.prompt_version)) {
    reasons.push("prompt_version metadata is required");
  }

  return reasons;
}

/**
 * Reads a staging row by ingest key.
 */
async function getStagingRow(pool: Pool, ingestKey: string): Promise<StagingRow | null> {
  const result = await pool.query<StagingRow>(
    `
      SELECT ingest_key, run_id, schema_version, prompt_version, reason, payload, status
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
 * Marks a pending staging row as failed.
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
 * Joins validation reasons into a single DB-safe reason string.
 */
function formatValidationReason(reasons: string[]): string {
  const joined = reasons.join("; ");
  return joined.length > 1000 ? `${joined.slice(0, 997)}...` : joined;
}

/**
 * Ensures Redis consumer group exists for pending stream.
 */
async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(
      STAGING_PENDING_STREAM,
      STAGING_STATE_RESOURCES_VALIDATOR_GROUP,
      "0",
      {
        MKSTREAM: true,
      }
    );
  } catch (error) {
    const message = toReason(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

/**
 * Reclaims stale pending entries for this consumer group so crashes don't strand messages.
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
      STAGING_PENDING_STREAM,
      STAGING_STATE_RESOURCES_VALIDATOR_GROUP,
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
 * Processes one pending stream message and updates staging status + routing stream.
 */
async function processMessage(
  pool: Pool,
  redis: ReturnType<typeof createClient>,
  messageId: string,
  message: Record<string, string>
): Promise<"validated" | "rejected" | "skipped" | "retry"> {
  const ingestKey = message.ingest_key;
  const messageRunId = normalizeRunId(message.run_id);

  if (!ingestKey) {
    await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
    return "skipped";
  }

  const row = await getStagingRow(pool, ingestKey);
  if (!row) {
    await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
    return "skipped";
  }

  if (hasRunIdMismatch(message.run_id, row.run_id)) {
    await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
    return "skipped";
  }
  const expectedRunId = messageRunId ?? normalizeRunId(row.run_id);

  // Recovery path: status changed but publish/ack may have failed in a prior attempt.
  if (row.status === "validated") {
    try {
      await redis.xAdd(STAGING_VALIDATED_STREAM, "*", {
        ingest_key: ingestKey,
        item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
        run_id: row.run_id ?? "",
      });
      await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
      return "validated";
    } catch {
      return "retry";
    }
  }

  // Recovery path for rejected items mirrors validated behavior.
  if (row.status === "rejected") {
    const reason = row.reason ?? "recovered rejected item (reason unavailable)";
    try {
      await redis.xAdd(STAGING_REJECTED_STREAM, "*", {
        ingest_key: ingestKey,
        item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
        run_id: row.run_id ?? "",
        reason,
      });
      await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
      return "rejected";
    } catch {
      return "retry";
    }
  }

  if (row.status !== "pending") {
    await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
    return "skipped";
  }

  const metadataReasons = validateStagingMetadata(row);
  const validation = validateStateResourcePayload(row.payload);

  if (metadataReasons.length === 0 && validation.ok) {
    const warningReason =
      validation.warnings.length > 0
        ? `[CONFLICT_WARN] ${formatValidationReason(validation.warnings)}`
        : null;

    const transition = await pool.query(
      `
        UPDATE staging_items
        SET status = 'validated',
            reason = $3,
            validated_at = now(),
            updated_at = now()
        WHERE ingest_key = $1
          AND item_type = $2
          AND status = 'pending'
          AND run_id IS NOT DISTINCT FROM $4
      `,
      [ingestKey, STAGING_ITEM_TYPE_STATE_RESOURCES, warningReason, expectedRunId]
    );

    if (transition.rowCount !== 1) {
      await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
      return "skipped";
    }

    try {
      // At-least-once: if publish succeeds but ack fails, downstream must dedupe by ingest_key.
      await redis.xAdd(STAGING_VALIDATED_STREAM, "*", {
        ingest_key: ingestKey,
        item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
        run_id: row.run_id ?? "",
      });

      if (warningReason) {
        console.warn(`validator conflict warning ingest_key=${ingestKey}: ${warningReason}`);
      }

      await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
      return "validated";
    } catch {
      // Leave unacked so XAUTOCLAIM can replay and recovery path can republish.
      return "retry";
    }
  }

  const reason = formatValidationReason([...metadataReasons, ...(validation.ok ? [] : validation.reasons)]);

  const transition = await pool.query(
    `
      UPDATE staging_items
      SET status = 'rejected',
          reason = $2,
          -- validated_at tracks validation-attempt time, including rejected rows.
          -- TODO: rename validated_at -> processed_at in a future migration for clarity.
          validated_at = now(),
          updated_at = now()
      WHERE ingest_key = $1
        AND item_type = $3
        AND status = 'pending'
        AND run_id IS NOT DISTINCT FROM $4
    `,
    [ingestKey, reason, STAGING_ITEM_TYPE_STATE_RESOURCES, expectedRunId]
  );

  if (transition.rowCount !== 1) {
    await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
    return "skipped";
  }

  try {
    await redis.xAdd(STAGING_REJECTED_STREAM, "*", {
      ingest_key: ingestKey,
      item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
      run_id: row.run_id ?? "",
      reason,
    });
    await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, messageId);
    return "rejected";
  } catch {
    // Leave unacked so XAUTOCLAIM can replay and recovery path can republish.
    return "retry";
  }
}

/**
 * Consumes pending state_resources items and routes them to validated or rejected.
 */
export async function runStateResourcesValidator(options: ValidatorOptions = {}): Promise<void> {
  const {
    once = false,
    batchSize = 20,
    blockMs = 5000,
  } = options;

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  await redis.connect();
  await ensureConsumerGroup(redis);

  const consumerName = `validator-${process.pid}`;

  let validated = 0;
  let rejected = 0;
  let skipped = 0;
  let retried = 0;

  const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
    for (const entry of entries) {
      try {
        const result = await processMessage(pool, redis, entry.id, entry.message);
        if (result === "validated") {
          validated += 1;
        } else if (result === "rejected") {
          rejected += 1;
        } else if (result === "retry") {
          retried += 1;
        } else {
          skipped += 1;
        }
      } catch (error) {
        const reason = toReason(error);
        const ingestKey = entry.message.ingest_key;

        if (!ingestKey) {
          try {
            await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, entry.id);
          } catch {
            retried += 1;
          }
          continue;
        }

        const status = await getStagingStatus(pool, ingestKey);
        if (status === "validated" || status === "rejected") {
          // Keep unacked so XAUTOCLAIM recovery can republish downstream event.
          retried += 1;
          continue;
        }

        if (status === "pending") {
          const row = await getStagingRow(pool, ingestKey);
          const expectedRunId = normalizeRunId(entry.message.run_id) ?? normalizeRunId(row?.run_id ?? null);
          await markFailedPending(pool, ingestKey, reason, expectedRunId);
        }

        try {
          await redis.xAck(STAGING_PENDING_STREAM, STAGING_STATE_RESOURCES_VALIDATOR_GROUP, entry.id);
        } catch {
          // Keep unacked for XAUTOCLAIM recovery.
          retried += 1;
        }
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
        STAGING_STATE_RESOURCES_VALIDATOR_GROUP,
        consumerName,
        [{ key: STAGING_PENDING_STREAM, id: ">" }],
        {
          COUNT: batchSize,
          BLOCK: blockMs,
        }
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
    `state_resources validator completed. validated=${validated} rejected=${rejected} skipped=${skipped} retried=${retried}`
  );
}
