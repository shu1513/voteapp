import { isIP } from "node:net";
import { lookup as dnsLookup } from "node:dns/promises";
import {
  STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
  STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
  STATE_RESOURCE_SOURCE_FIELDS,
} from "../contracts/stateResourceEnrichmentContract.js";
import type { PipelineEnv } from "../config/env.js";
import type {
  AiProvider,
  EvidenceSnippet,
  EnrichStateResourcesConfig,
  EnrichStateResourcesInput,
  EnrichStateResourcesResult,
  ProviderAdapter,
} from "./types.js";
import {
  parseStateResourceGroupPayloadFromAi,
  parseStateResourcePayloadFromAi,
} from "./stateResourcePayloadValidation.js";
import { openAiProvider } from "./providers/openaiProvider.js";
import { claudeProvider } from "./providers/claudeProvider.js";
import { geminiProvider } from "./providers/geminiProvider.js";
import type { StateResourcePayload, StateResourceSources } from "../types/stateResource.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";
import {
  getStateResourceFieldGroupConfig,
  type StateResourceFieldGroup,
} from "./stateResourceFieldGroups.js";

const PROVIDER_ADAPTERS: Record<AiProvider, ProviderAdapter> = {
  openai: openAiProvider,
  claude: claudeProvider,
  gemini: geminiProvider,
};

const CITATION_FETCH_TIMEOUT_MS = 8_000;
const CITATION_MAX_RESPONSE_BYTES = 1_000_000;
type ScopedGroupPayload = Partial<Omit<StateResourcePayload, "sources">> & {
  sources: Partial<StateResourceSources>;
};

function getHostname(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
}

function normalizeWhitespace(input: string): string {
  return input.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim();
}

function isPrivateIpLiteral(hostnameOrIp: string): boolean {
  const host = hostnameOrIp.toLowerCase().replace(/^\[|\]$/g, "");
  const ipVersion = isIP(host);
  if (ipVersion === 4) {
    const octets = host.split(".").map((part) => Number.parseInt(part, 10));
    if (octets.length !== 4 || octets.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) {
      return true;
    }

    const [a, b] = octets;
    return (
      a === 10 ||
      a === 127 ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      a === 0 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 198 && (b === 18 || b === 19)) ||
      a >= 224
    );
  }

  if (ipVersion === 6) {
    return (
      host === "::1" ||
      host === "::" ||
      host.startsWith("fc") ||
      host.startsWith("fd") ||
      host.startsWith("fe8") ||
      host.startsWith("fe9") ||
      host.startsWith("fea") ||
      host.startsWith("feb")
    );
  }

  return false;
}

function isBlockedCitationHostname(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (
    host === "localhost" ||
    host.endsWith(".localhost") ||
    host.endsWith(".local") ||
    host.endsWith(".internal") ||
    host === "metadata.google.internal" ||
    host === "metadata"
  ) {
    return true;
  }

  return isPrivateIpLiteral(host);
}

async function resolvesToBlockedPrivateIp(hostname: string): Promise<boolean> {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (!host || isIP(host) > 0) {
    return false;
  }

  try {
    const records = await dnsLookup(host, {
      all: true,
      verbatim: true,
    });
    return records.some((record) => isPrivateIpLiteral(record.address));
  } catch {
    // Best-effort DNS safety check: keep flow resilient if DNS resolution is unavailable.
    return false;
  }
}

function isAllowedCitationContentType(contentType: string): boolean {
  const lower = contentType.toLowerCase();
  if (!lower) {
    return false;
  }

  return (
    lower.includes("text/html") ||
    lower.includes("text/plain") ||
    lower.includes("application/json") ||
    lower.includes("application/xml") ||
    lower.includes("text/xml")
  );
}

function stripHtmlToText(input: string): string {
  const withoutScripts = input.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ");
  const withoutStyles = withoutScripts.replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ");
  const withoutTags = withoutStyles.replace(/<[^>]+>/g, " ");
  return normalizeWhitespace(withoutTags);
}

async function fetchCitationEvidenceSnippet(
  citationUrl: string,
  fallbackSourceName: string
): Promise<{ ok: true; snippet: EvidenceSnippet } | { ok: false; reason: string }> {
  const normalizedInputUrl = normalizeHttpUrl(citationUrl);
  if (!normalizedInputUrl) {
    return { ok: false, reason: "citation URL is not a valid http(s) URL" };
  }

  let inputParsed: URL;
  try {
    inputParsed = new URL(normalizedInputUrl);
  } catch {
    return { ok: false, reason: "citation URL is not parseable" };
  }

  if (isBlockedCitationHostname(inputParsed.hostname)) {
    return { ok: false, reason: "citation URL points to a blocked/private host" };
  }
  if (await resolvesToBlockedPrivateIp(inputParsed.hostname)) {
    return { ok: false, reason: "citation URL hostname resolves to a blocked/private IP" };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CITATION_FETCH_TIMEOUT_MS);
  let response: Response | null = null;

  try {
    response = await fetch(normalizedInputUrl, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
    });

    if (!response.ok) {
      if (response.status === 403) {
        const finalUrl = normalizeHttpUrl(response.url || normalizedInputUrl);
        if (!finalUrl) {
          return { ok: false, reason: "citation final URL is invalid after redirects" };
        }

        let finalParsed: URL;
        try {
          finalParsed = new URL(finalUrl);
        } catch {
          return { ok: false, reason: "citation final URL is not parseable" };
        }

        if (isBlockedCitationHostname(finalParsed.hostname)) {
          return { ok: false, reason: "citation final URL points to a blocked/private host" };
        }
        if (await resolvesToBlockedPrivateIp(finalParsed.hostname)) {
          return { ok: false, reason: "citation final URL hostname resolves to a blocked/private IP" };
        }

        const sourceName = normalizeWhitespace(fallbackSourceName) || getHostname(finalUrl) || "source";
        return {
          ok: true,
          snippet: {
            url: finalUrl,
            title: sourceName,
          },
        };
      }
      return { ok: false, reason: `citation fetch returned status ${response.status}` };
    }

    const finalUrl = normalizeHttpUrl(response.url || normalizedInputUrl);
    if (!finalUrl) {
      return { ok: false, reason: "citation final URL is invalid after redirects" };
    }

    let finalParsed: URL;
    try {
      finalParsed = new URL(finalUrl);
    } catch {
      return { ok: false, reason: "citation final URL is not parseable" };
    }

    if (isBlockedCitationHostname(finalParsed.hostname)) {
      return { ok: false, reason: "citation final URL points to a blocked/private host" };
    }
    if (await resolvesToBlockedPrivateIp(finalParsed.hostname)) {
      return { ok: false, reason: "citation final URL hostname resolves to a blocked/private IP" };
    }

    const contentType = response.headers.get("content-type") ?? "";
    if (!isAllowedCitationContentType(contentType)) {
      return { ok: false, reason: "citation URL response content-type is not allowed" };
    }

    const contentLengthRaw = response.headers.get("content-length");
    if (contentLengthRaw) {
      const contentLength = Number.parseInt(contentLengthRaw, 10);
      if (Number.isFinite(contentLength) && contentLength > CITATION_MAX_RESPONSE_BYTES) {
        return { ok: false, reason: "citation URL response is too large" };
      }
    }

    const sourceName = normalizeWhitespace(fallbackSourceName) || getHostname(finalUrl) || "source";

    return {
      ok: true,
      snippet: {
        url: finalUrl,
        title: sourceName,
      },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.toLowerCase().includes("aborted")) {
      return { ok: false, reason: "citation URL fetch timed out" };
    }
    return { ok: false, reason: `citation URL fetch failed: ${message}` };
  } finally {
    // We validate headers/url only; always cancel body to release sockets promptly.
    if (response) {
      try {
        await response.body?.cancel();
      } catch {
        // best effort: do not mask the primary result
      }
    }
    clearTimeout(timeout);
  }
}

async function verifyAndCollectAdditionalCitationEvidence(
  payload: StateResourcePayload,
  evidence: EnrichStateResourcesInput["evidence"]
): Promise<
  | { ok: true; verifiedCitationEvidence: EvidenceSnippet[] }
  | {
      ok: false;
      reason: string;
      failedCitationUrls: string[];
      failures: Array<{ field: (typeof STATE_RESOURCE_SOURCE_FIELDS)[number]; url: string; reason: string }>;
    }
> {
  const knownEvidenceUrls = new Set(
    evidence
      .map((item) => normalizeHttpUrl(item.url))
      .filter((url): url is string => typeof url === "string")
  );
  const verifiedCitationEvidence: EvidenceSnippet[] = [];
  const seenNewCitationUrls = new Set<string>();
  const verificationFailures: Array<{
    field: (typeof STATE_RESOURCE_SOURCE_FIELDS)[number];
    url: string;
    reason: string;
  }> = [];

  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    for (const citation of payload.sources[key]) {
      const normalizedCitationUrl = normalizeHttpUrl(citation);
      if (!normalizedCitationUrl) {
        verificationFailures.push({
          field: key,
          url: citation,
          reason: "invalid citation URL",
        });
        continue;
      }

      if (knownEvidenceUrls.has(normalizedCitationUrl) || seenNewCitationUrls.has(normalizedCitationUrl)) {
        continue;
      }

      const fetched = await fetchCitationEvidenceSnippet(normalizedCitationUrl, getHostname(normalizedCitationUrl));
      if (!fetched.ok) {
        verificationFailures.push({
          field: key,
          url: normalizedCitationUrl,
          reason: fetched.reason,
        });
        continue;
      }

      seenNewCitationUrls.add(fetched.snippet.url);
      knownEvidenceUrls.add(fetched.snippet.url);
      verifiedCitationEvidence.push(fetched.snippet);
    }
  }

  if (verificationFailures.length > 0) {
    const failedCitationUrls = Array.from(new Set(verificationFailures.map((failure) => failure.url))).slice(0, 100);
    const reasonPreview = verificationFailures
      .slice(0, 3)
      .map((failure) => `sources.${failure.field} (${failure.url}): ${failure.reason}`)
      .join("; ");
    const extraCount = verificationFailures.length > 3 ? ` (+${verificationFailures.length - 3} more)` : "";

    return {
      ok: false,
      reason: `citation URL(s) could not be verified for ${verificationFailures.length} citation(s): ${reasonPreview}${extraCount}`,
      failedCitationUrls,
      failures: verificationFailures,
    };
  }

  return { ok: true, verifiedCitationEvidence };
}

async function verifyAndCollectAdditionalCitationEvidenceForFields(
  sources: Partial<StateResourceSources>,
  sourceFields: readonly (keyof StateResourceSources)[],
  evidence: EnrichStateResourcesInput["evidence"]
): Promise<
  | { ok: true; verifiedCitationEvidence: EvidenceSnippet[] }
  | {
      ok: false;
      reason: string;
      failedCitationUrls: string[];
      failures: Array<{ field: keyof StateResourceSources; url: string; reason: string }>;
    }
> {
  const knownEvidenceUrls = new Set(
    evidence
      .map((item) => normalizeHttpUrl(item.url))
      .filter((url): url is string => typeof url === "string")
  );
  const verifiedCitationEvidence: EvidenceSnippet[] = [];
  const seenNewCitationUrls = new Set<string>();
  const verificationFailures: Array<{
    field: keyof StateResourceSources;
    url: string;
    reason: string;
  }> = [];

  for (const key of sourceFields) {
    const citations = sources[key];
    if (!Array.isArray(citations) || citations.length === 0) {
      verificationFailures.push({
        field: key,
        url: "",
        reason: "missing required source citations",
      });
      continue;
    }

    for (const citation of citations) {
      const normalizedCitationUrl = normalizeHttpUrl(citation);
      if (!normalizedCitationUrl) {
        verificationFailures.push({
          field: key,
          url: citation,
          reason: "invalid citation URL",
        });
        continue;
      }

      if (knownEvidenceUrls.has(normalizedCitationUrl) || seenNewCitationUrls.has(normalizedCitationUrl)) {
        continue;
      }

      const fetched = await fetchCitationEvidenceSnippet(normalizedCitationUrl, getHostname(normalizedCitationUrl));
      if (!fetched.ok) {
        verificationFailures.push({
          field: key,
          url: normalizedCitationUrl,
          reason: fetched.reason,
        });
        continue;
      }

      seenNewCitationUrls.add(fetched.snippet.url);
      knownEvidenceUrls.add(fetched.snippet.url);
      verifiedCitationEvidence.push(fetched.snippet);
    }
  }

  if (verificationFailures.length > 0) {
    const failedCitationUrls = Array.from(
      new Set(
        verificationFailures
          .map((failure) => failure.url)
          .filter((url) => url.length > 0)
      )
    ).slice(0, 100);
    const reasonPreview = verificationFailures
      .slice(0, 3)
      .map((failure) => `sources.${failure.field}${failure.url ? ` (${failure.url})` : ""}: ${failure.reason}`)
      .join("; ");
    const extraCount = verificationFailures.length > 3 ? ` (+${verificationFailures.length - 3} more)` : "";

    return {
      ok: false,
      reason: `citation URL(s) could not be verified for ${verificationFailures.length} citation(s): ${reasonPreview}${extraCount}`,
      failedCitationUrls,
      failures: verificationFailures,
    };
  }

  return { ok: true, verifiedCitationEvidence };
}

/**
 * Builds normalized evidence URL set and validates evidence preconditions.
 */
function buildEvidenceUrlSet(
  evidence: EnrichStateResourcesInput["evidence"]
): { ok: true } | { ok: false; reason: string } {
  if (!Array.isArray(evidence)) {
    return { ok: false, reason: "evidence must be an array" };
  }

  // Evidence can be empty (seed URLs may be blocked/unreadable). In that case,
  // the model can still research and we verify returned citations independently.
  if (evidence.length === 0) {
    return { ok: true };
  }

  const urlSet = new Set(
    evidence
      .map((item) => normalizeHttpUrl(item.url))
      .filter((url): url is string => typeof url === "string")
  );

  if (urlSet.size === 0) {
    return { ok: false, reason: "evidence entries must contain valid http(s) URLs" };
  }

  return { ok: true };
}

/**
 * Ensures every citation URL is a valid normalized http(s) URL.
 * Seed URLs are starting points for research, not a hard citation allowlist.
 */
function validateCitationUrls(payload: StateResourcePayload): string | null {
  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    for (const citation of payload.sources[key]) {
      const normalizedCitationUrl = normalizeHttpUrl(citation);
      if (!normalizedCitationUrl) {
        return `sources.${key} contains an invalid citation URL`;
      }
    }
  }

  return null;
}

/**
 * Normalizes AI citations and deduplicates URL entries.
 * Applies deterministic fallbacks only for URL fields (not legal summary text fields).
 */
function groundCitationsToEvidence(
  payload: StateResourcePayload
): StateResourcePayload {
  const groundedSources = {} as StateResourcePayload["sources"];

  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const seen = new Set<string>();
    const grounded = payload.sources[key]
      .map((citation) => {
        const normalized = normalizeHttpUrl(citation);
        if (!normalized || seen.has(normalized)) {
          return null;
        }
        seen.add(normalized);
        return normalized;
      })
      .filter((citation): citation is string => citation !== null);

    groundedSources[key] = grounded;
  }

  return {
    ...payload,
    sources: groundedSources,
  };
}

/**
 * Builds enrichment runtime config from environment.
 */
export function buildEnrichmentConfigFromEnv(env: PipelineEnv): EnrichStateResourcesConfig {
  return {
    provider: env.AI_PROVIDER,
    model: env.AI_MODEL,
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

/**
 * Model-agnostic state_resources enrichment interface.
 * This function is the single entry point regardless of provider.
 */
export async function enrichStateResources(
  input: EnrichStateResourcesInput,
  config: EnrichStateResourcesConfig
): Promise<EnrichStateResourcesResult> {
  const adapter = PROVIDER_ADAPTERS[config.provider];

  if (!adapter) {
    return {
      ok: false,
      retryable: false,
      errorCode: "UNSUPPORTED_PROVIDER",
      reason: `Unsupported AI provider: ${config.provider}`,
    };
  }

  // Fail fast on unusable evidence before spending provider latency/tokens.
  const evidenceCheck = buildEvidenceUrlSet(input.evidence);
  if (!evidenceCheck.ok) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: evidenceCheck.reason,
    };
  }

  const generated = await adapter(input, config);
  if (!generated.ok) {
    return generated;
  }

  const providerFailureDebug = {
    draft_snapshot: input.draft,
    retry_feedback: input.retryFeedback ?? null,
    ...(generated.debugMeta ?? {}),
    provider_response_text: generated.rawText ?? null,
    provider_response_payload: generated.rawPayload,
  } as const;

  const parsed = parseStateResourcePayloadFromAi(generated.rawPayload);
  if (!parsed.ok) {
    return {
      ok: false,
      retryable: false,
      errorCode: parsed.errorCode,
      reason: parsed.reason,
      failureDebug: providerFailureDebug,
    };
  }

  const aiRawDebug = {
    draft_snapshot: input.draft,
    retry_feedback: input.retryFeedback ?? null,
    ...(generated.debugMeta ?? {}),
    provider_response_text: generated.rawText ?? null,
    provider_response_payload: generated.rawPayload,
    ai_payload_before_grounding: parsed.payload,
  } as const;

  const expectedStateFips = input.draft.state_fips.trim();
  const expectedStateAbbreviation = input.draft.state_abbreviation.trim();
  const expectedStateName = input.draft.state_name.trim();

  let normalizedPayload = groundCitationsToEvidence(parsed.payload);
  const normalizedPollingPlaceUrl = normalizeHttpUrl(normalizedPayload.polling_place_url);
  const groundedPollingPlaceUrl =
    normalizedPayload.sources.polling_place_url.find(
      (citation) => normalizeHttpUrl(citation) === normalizedPollingPlaceUrl
    ) ??
    normalizedPayload.sources.polling_place_url[0] ??
    normalizedPayload.polling_place_url;
  normalizedPayload = {
    ...normalizedPayload,
    polling_place_url: groundedPollingPlaceUrl,
    state_fips: expectedStateFips,
    state_abbreviation: expectedStateAbbreviation,
    state_name: expectedStateName,
    voter_registration_url: STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
  };

  for (const field of STATE_RESOURCE_SOURCE_FIELDS) {
    if (normalizedPayload.sources[field].length === 0) {
      return {
        ok: false,
        retryable: false,
        errorCode: "SCHEMA_MISMATCH",
        reason: `sources.${field} must include at least one citation URL`,
        failureDebug: providerFailureDebug,
      };
    }
  }

  const citationUrlReason = validateCitationUrls(normalizedPayload);
  if (citationUrlReason) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: citationUrlReason,
      failureDebug: providerFailureDebug,
    };
  }

  const citationEvidenceResult = await verifyAndCollectAdditionalCitationEvidence(normalizedPayload, input.evidence);
  if (!citationEvidenceResult.ok) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: citationEvidenceResult.reason,
      failureDebug: {
        ...providerFailureDebug,
        failed_citation_urls: citationEvidenceResult.failedCitationUrls,
        citation_verification_failures: citationEvidenceResult.failures,
      },
    };
  }

  return {
    ok: true,
    payload: normalizedPayload,
    schemaVersion: STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
    provider: config.provider,
    model: config.model,
    promptVersion: input.promptVersion,
    aiRawDebug,
    verifiedCitationEvidence: citationEvidenceResult.verifiedCitationEvidence,
  };
}

export type EnrichStateResourceGroupResult =
  | {
      ok: true;
      fieldGroup: StateResourceFieldGroup;
      payload: ScopedGroupPayload;
      provider: AiProvider;
      model: string;
      promptVersion: string;
      aiRawDebug: Record<string, unknown> | null;
      verifiedCitationEvidence: EvidenceSnippet[];
    }
  | {
      ok: false;
      retryable: boolean;
      reason: string;
      errorCode:
        | "RATE_LIMIT"
        | "TIMEOUT"
        | "TEMP_PROVIDER_ERROR"
        | "INVALID_JSON"
        | "SCHEMA_MISMATCH"
        | "MISSING_REQUIRED_FIELDS"
        | "CONFIGURATION_ERROR"
        | "UNSUPPORTED_PROVIDER";
      failureDebug?: Record<string, unknown>;
    };

export async function enrichStateResourcesGroup(
  input: EnrichStateResourcesInput & { fieldGroup: StateResourceFieldGroup },
  config: EnrichStateResourcesConfig
): Promise<EnrichStateResourceGroupResult> {
  const adapter = PROVIDER_ADAPTERS[config.provider];
  if (!adapter) {
    return {
      ok: false,
      retryable: false,
      errorCode: "UNSUPPORTED_PROVIDER",
      reason: `Unsupported AI provider: ${config.provider}`,
    };
  }

  const evidenceCheck = buildEvidenceUrlSet(input.evidence);
  if (!evidenceCheck.ok) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: evidenceCheck.reason,
    };
  }

  const generated = await adapter(input, config);
  if (!generated.ok) {
    return generated;
  }

  const providerFailureDebug = {
    draft_snapshot: input.draft,
    retry_feedback: input.retryFeedback ?? null,
    field_group: input.fieldGroup,
    ...(generated.debugMeta ?? {}),
    provider_response_text: generated.rawText ?? null,
    provider_response_payload: generated.rawPayload,
  } as const;

  const parsed = parseStateResourceGroupPayloadFromAi(generated.rawPayload, input.fieldGroup);
  if (!parsed.ok) {
    return {
      ok: false,
      retryable: false,
      errorCode: parsed.errorCode,
      reason: parsed.reason,
      failureDebug: providerFailureDebug,
    };
  }

  const aiRawDebug = {
    draft_snapshot: input.draft,
    retry_feedback: input.retryFeedback ?? null,
    field_group: input.fieldGroup,
    ...(generated.debugMeta ?? {}),
    provider_response_text: generated.rawText ?? null,
    provider_response_payload: generated.rawPayload,
    ai_payload_before_grounding: parsed.payload,
  } as const;

  const groupConfig = getStateResourceFieldGroupConfig(input.fieldGroup);
  const groundedSources = {} as Partial<StateResourceSources>;
  const knownEvidenceUrls = new Set(
    input.evidence
      .map((item) => normalizeHttpUrl(item.url))
      .filter((url): url is string => typeof url === "string")
  );

  for (const key of groupConfig.sourceKeys) {
    const citations = parsed.payload.sources[key] ?? [];
    const seen = new Set<string>();
    const normalized = citations
      .map((citation) => {
        const normalized = normalizeHttpUrl(citation);
        if (!normalized || seen.has(normalized)) {
          return null;
        }
        seen.add(normalized);
        return normalized;
      })
      .filter((citation): citation is string => citation !== null);

    // Preserve all citations but prefer evidence-backed URLs first.
    const evidenceBacked = normalized.filter((url) => knownEvidenceUrls.has(url));
    const additionalCitations = normalized.filter((url) => !knownEvidenceUrls.has(url));
    groundedSources[key] = [...evidenceBacked, ...additionalCitations];
  }

  const citationEvidenceResult = await verifyAndCollectAdditionalCitationEvidenceForFields(
    groundedSources,
    groupConfig.sourceKeys,
    input.evidence
  );
  if (!citationEvidenceResult.ok) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: citationEvidenceResult.reason,
      failureDebug: {
        ...providerFailureDebug,
        failed_citation_urls: citationEvidenceResult.failedCitationUrls,
        citation_verification_failures: citationEvidenceResult.failures,
      },
    };
  }

  return {
    ok: true,
    fieldGroup: input.fieldGroup,
    payload: {
      ...parsed.payload,
      sources: groundedSources,
    },
    provider: config.provider,
    model: config.model,
    promptVersion: input.promptVersion,
    aiRawDebug,
    verifiedCitationEvidence: citationEvidenceResult.verifiedCitationEvidence,
  };
}
