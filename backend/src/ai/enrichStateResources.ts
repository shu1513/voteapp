import { isIP } from "node:net";
import { lookup as dnsLookup } from "node:dns/promises";
import {
  STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
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
import { parseStateResourcePayloadFromAi } from "./stateResourcePayloadValidation.js";
import { openAiProvider } from "./providers/openaiProvider.js";
import { claudeProvider } from "./providers/claudeProvider.js";
import { geminiProvider } from "./providers/geminiProvider.js";
import type { StateResourcePayload } from "../types/stateResource.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";
import { isLikelyPollingPlaceUrl as isLikelyPollingPlaceUrlByUrl } from "../utils/isLikelyPollingPlaceUrl.js";
import { CURATED_STATE_POLLING_URL_BY_FIPS } from "../constants/curatedPollingUrls.js";

const PROVIDER_ADAPTERS: Record<AiProvider, ProviderAdapter> = {
  openai: openAiProvider,
  claude: claudeProvider,
  gemini: geminiProvider,
};

const AGGREGATOR_HOSTS = new Set([
  "vote.org",
  "www.vote.org",
  "nass.org",
  "www.nass.org",
  "usvotefoundation.org",
  "www.usvotefoundation.org",
]);

const PREFERRED_OFFICIAL_CITATION_FIELDS = new Set<
  "vote_by_mail_info" | "polling_hours" | "id_requirements"
>(["vote_by_mail_info", "polling_hours", "id_requirements"]);
type PreferredOfficialCitationField = "vote_by_mail_info" | "polling_hours" | "id_requirements";
const LEGAL_SUMMARY_CITATION_FIELDS = new Set<PreferredOfficialCitationField>([
  "vote_by_mail_info",
  "polling_hours",
  "id_requirements",
]);
const CITATION_FETCH_TIMEOUT_MS = 8_000;
const CITATION_MAX_RESPONSE_BYTES = 1_000_000;

function getHostname(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
}

function isAggregatorUrl(url: string): boolean {
  const host = getHostname(url);
  return AGGREGATOR_HOSTS.has(host);
}

function isPreferredOfficialCitationField(
  field: (typeof STATE_RESOURCE_SOURCE_FIELDS)[number]
): field is PreferredOfficialCitationField {
  return PREFERRED_OFFICIAL_CITATION_FIELDS.has(field as PreferredOfficialCitationField);
}

function isLegalSummaryCitationField(
  field: (typeof STATE_RESOURCE_SOURCE_FIELDS)[number]
): field is PreferredOfficialCitationField {
  return LEGAL_SUMMARY_CITATION_FIELDS.has(field as PreferredOfficialCitationField);
}

function getPathname(url: string): string {
  try {
    return new URL(url).pathname.toLowerCase();
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

  try {
    const response = await fetch(normalizedInputUrl, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
    });

    if (!response.ok) {
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

    const bodyText = await response.text();
    if (bodyText.length > CITATION_MAX_RESPONSE_BYTES) {
      return { ok: false, reason: "citation URL response body is too large" };
    }

    const textForSnippet = contentType.toLowerCase().includes("text/html")
      ? stripHtmlToText(bodyText)
      : normalizeWhitespace(bodyText);

    if (!textForSnippet) {
      return { ok: false, reason: "citation URL did not provide readable text content" };
    }

    const titleMatch = /<title\b[^>]*>([\s\S]*?)<\/title>/i.exec(bodyText);
    const extractedTitle = titleMatch ? normalizeWhitespace(stripHtmlToText(titleMatch[1])) : "";
    const sourceName = extractedTitle || normalizeWhitespace(fallbackSourceName) || getHostname(finalUrl) || "source";

    return {
      ok: true,
      snippet: {
        url: finalUrl,
        title: sourceName,
        snippet: textForSnippet.slice(0, 800),
      },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.toLowerCase().includes("aborted")) {
      return { ok: false, reason: "citation URL fetch timed out" };
    }
    return { ok: false, reason: `citation URL fetch failed: ${message}` };
  } finally {
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
      const normalizedCitationUrl = normalizeHttpUrl(citation.source_url);
      if (!normalizedCitationUrl) {
        verificationFailures.push({
          field: key,
          url: citation.source_url,
          reason: "invalid citation URL",
        });
        continue;
      }

      if (knownEvidenceUrls.has(normalizedCitationUrl) || seenNewCitationUrls.has(normalizedCitationUrl)) {
        continue;
      }

      const fetched = await fetchCitationEvidenceSnippet(normalizedCitationUrl, citation.source_name);
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

function isOfficialElectionSource(url: string, sourceName = "", snippet = ""): boolean {
  const host = getHostname(url);
  const pathname = getPathname(url);
  const text = `${host} ${pathname} ${sourceName} ${snippet}`.toLowerCase();

  if (host.endsWith(".gov")) {
    return true;
  }

  if (/(sos|secretary-?of-?state|board-?of-?elections|county clerk|elections?)/i.test(text)) {
    return true;
  }

  return false;
}

function hasStateSignal(url: string, title: string, snippet: string, stateName: string, stateAbbreviation: string): boolean {
  const lowerUrl = url.toLowerCase();
  const stateNameLower = stateName.trim().toLowerCase();
  const stateSlug = stateNameLower.replace(/\s+/g, "-");
  const stateCompact = stateNameLower.replace(/[^a-z0-9]/g, "");
  const urlCompact = lowerUrl.replace(/[^a-z0-9]/g, "");
  const abbreviationLower = stateAbbreviation.trim().toLowerCase();
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

function isLikelyPollingPlaceUrl(url: string, title: string, snippet: string): boolean {
  if (isLikelyPollingPlaceUrlByUrl(url)) {
    return true;
  }

  const combined = `${url} ${title} ${snippet}`.toLowerCase();

  if (/\b(polling|polling-place|polling place|find-your-polling-place|locator)\b/.test(combined)) {
    return true;
  }

  if (/\b(register|registration|absentee|mail|id[-\s]?laws?|identification)\b/.test(combined)) {
    return false;
  }

  return false;
}

function scorePollingCandidate(url: string, stateSignal: boolean): number {
  const host = getHostname(url);
  let score = 0;

  if (host.endsWith(".gov")) {
    score += 50;
  }

  if (/(sos|secretary-?of-?state|elections?)/i.test(host + url)) {
    score += 20;
  }

  if (/polling|polling-place|find-your-polling-place|locator/i.test(url)) {
    score += 10;
  }

  if (isAggregatorUrl(url)) {
    score -= 40;
  }

  if (stateSignal) {
    score += 120;
  } else if (!isAggregatorUrl(url)) {
    // Strongly discourage official URLs that point to a different state.
    score -= 220;
  }

  return score;
}

/**
 * When evidence contains both aggregator and official polling URLs, prefer the official URL.
 */
function preferOfficialPollingPlaceUrl(
  payload: StateResourcePayload,
  evidence: EnrichStateResourcesInput["evidence"],
  draft: EnrichStateResourcesInput["draft"]
): StateResourcePayload {
  const normalizedCurrent = normalizeHttpUrl(payload.polling_place_url);
  if (!normalizedCurrent) {
    return payload;
  }
  const currentLooksLikePollingUrl = isLikelyPollingPlaceUrlByUrl(normalizedCurrent);
  const mustCorrectCurrent = !currentLooksLikePollingUrl;
  const currentHasStateSignal = hasStateSignal(normalizedCurrent, "", "", draft.state_name, draft.state_abbreviation);
  if (!isAggregatorUrl(normalizedCurrent) && currentLooksLikePollingUrl && currentHasStateSignal) {
    return payload;
  }

  const candidates = evidence
    .map((item) => {
      const normalized = normalizeHttpUrl(item.url);
      if (!normalized) {
        return null;
      }

      if (!isLikelyPollingPlaceUrl(normalized, item.title, item.snippet)) {
        return null;
      }

      return {
        normalizedUrl: normalized,
        score: scorePollingCandidate(
          normalized,
          hasStateSignal(normalized, item.title, item.snippet, draft.state_name, draft.state_abbreviation)
        ),
        sourceName: item.title.trim().length > 0 ? item.title.trim() : getHostname(normalized),
      };
    })
    .filter((item): item is { normalizedUrl: string; score: number; sourceName: string } => item !== null)
    .sort((a, b) => b.score - a.score);

  const best = candidates[0];
  if (!best) {
    return payload;
  }

  // If current URL is valid but low quality (aggregator), only replace with a strong non-aggregator candidate.
  if (!mustCorrectCurrent && currentHasStateSignal && (best.score <= 0 || isAggregatorUrl(best.normalizedUrl))) {
    return payload;
  }

  const hasCitation = payload.sources.polling_place_url.some((citation) => {
    const normalizedCitationUrl = normalizeHttpUrl(citation.source_url);
    return normalizedCitationUrl === best.normalizedUrl;
  });

  const pollingPlaceCitations = hasCitation
    ? payload.sources.polling_place_url
    : [
        {
          source_name: best.sourceName.length > 0 ? best.sourceName : getHostname(best.normalizedUrl),
          source_url: best.normalizedUrl,
        },
        ...payload.sources.polling_place_url,
      ];

  return {
    ...payload,
    polling_place_url: best.normalizedUrl,
    sources: {
      ...payload.sources,
      polling_place_url: pollingPlaceCitations,
    },
  };
}

/**
 * Builds normalized evidence URL set and validates evidence preconditions.
 */
function buildEvidenceUrlSet(
  evidence: EnrichStateResourcesInput["evidence"]
): { ok: true } | { ok: false; reason: string } {
  if (!Array.isArray(evidence) || evidence.length === 0) {
    return { ok: false, reason: "evidence snippets are required for citation grounding" };
  }

  const urlSet = new Set(
    evidence
      .map((item) => normalizeHttpUrl(item.url))
      .filter((url): url is string => typeof url === "string")
  );

  if (urlSet.size === 0) {
    return { ok: false, reason: "evidence snippets must contain valid http(s) URLs" };
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
      const normalizedCitationUrl = normalizeHttpUrl(citation.source_url);
      if (!normalizedCitationUrl) {
        return `sources.${key} contains an invalid citation URL`;
      }
    }
  }

  return null;
}

function chooseFallbackEvidenceUrl(
  field: (typeof STATE_RESOURCE_SOURCE_FIELDS)[number],
  evidence: EnrichStateResourcesInput["evidence"]
): { url: string; sourceName: string } | null {
  const scored = evidence
    .map((item) => {
      const normalizedUrl = normalizeHttpUrl(item.url);
      if (!normalizedUrl) {
        return null;
      }

      const lower = `${normalizedUrl} ${item.title} ${item.snippet}`.toLowerCase();
      let score = 0;

      if (field === "polling_place_url") {
        if (isLikelyPollingPlaceUrl(normalizedUrl, item.title, item.snippet)) {
          score += 100;
        }
      } else if (field === "voter_registration_url") {
        if (/\b(register|registration|voter registration)\b/.test(lower)) {
          score += 100;
        }
      } else if (field === "vote_by_mail_info") {
        if (/\b(absentee|mail ballot|vote by mail|vote-by-mail|drop box|postmark)\b/.test(lower)) {
          score += 100;
        }
      } else if (field === "polling_hours") {
        if (/\b(hours|open|close|polling hours|election day)\b/.test(lower)) {
          score += 100;
        }
      } else if (field === "id_requirements") {
        if (/\b(voter id|id law|identification|photo id)\b/.test(lower)) {
          score += 100;
        }
      }

      if (isPreferredOfficialCitationField(field) && isOfficialElectionSource(normalizedUrl, item.title, item.snippet)) {
        score += 40;
      }

      if (score === 0) {
        return null;
      }

      return {
        url: normalizedUrl,
        sourceName: item.title.trim().length > 0 ? item.title.trim() : getHostname(normalizedUrl),
        score,
      };
    })
    .filter((item): item is { url: string; sourceName: string; score: number } => item !== null)
    .sort((a, b) => b.score - a.score);

  if (scored.length > 0) {
    return { url: scored[0].url, sourceName: scored[0].sourceName };
  }

  const first = evidence
    .map((item) => {
      const normalizedUrl = normalizeHttpUrl(item.url);
      if (!normalizedUrl) {
        return null;
      }
      return {
        url: normalizedUrl,
        sourceName: item.title.trim().length > 0 ? item.title.trim() : getHostname(normalizedUrl),
      };
    })
    .find((item): item is { url: string; sourceName: string } => item !== null);

  return first ?? null;
}

function choosePreferredOfficialCitationForField(
  field: "vote_by_mail_info" | "polling_hours" | "id_requirements",
  evidence: EnrichStateResourcesInput["evidence"]
): { url: string; sourceName: string } | null {
  const ranked = evidence
    .map((item) => {
      const normalizedUrl = normalizeHttpUrl(item.url);
      if (!normalizedUrl) {
        return null;
      }
      if (!isOfficialElectionSource(normalizedUrl, item.title, item.snippet)) {
        return null;
      }

      const lower = `${normalizedUrl} ${item.title} ${item.snippet}`.toLowerCase();
      let relevance = 0;

      if (field === "vote_by_mail_info" && /\b(absentee|mail ballot|vote by mail|vote-by-mail|drop box|postmark|deadline)\b/.test(lower)) {
        relevance += 100;
      }
      if (field === "polling_hours" && /\b(hours|open|close|polling hours|election day)\b/.test(lower)) {
        relevance += 100;
      }
      if (field === "id_requirements" && /\b(voter id|id law|identification|photo id)\b/.test(lower)) {
        relevance += 100;
      }

      return {
        url: normalizedUrl,
        sourceName: item.title.trim().length > 0 ? item.title.trim() : getHostname(normalizedUrl),
        score: relevance,
      };
    })
    .filter(
      (item): item is { url: string; sourceName: string; score: number } =>
        item !== null && item.score > 0
    )
    .sort((a, b) => b.score - a.score);

  if (ranked.length === 0) {
    return null;
  }

  return {
    url: ranked[0].url,
    sourceName: ranked[0].sourceName,
  };
}

function getEvidenceSourceNameForUrl(
  targetUrl: string,
  evidence: EnrichStateResourcesInput["evidence"]
): string | null {
  const normalizedTarget = normalizeHttpUrl(targetUrl);
  if (!normalizedTarget) {
    return null;
  }

  for (const item of evidence) {
    const normalizedEvidenceUrl = normalizeHttpUrl(item.url);
    if (normalizedEvidenceUrl !== normalizedTarget) {
      continue;
    }

    const trimmedTitle = item.title.trim();
    if (trimmedTitle.length > 0) {
      return trimmedTitle;
    }

    return getHostname(normalizedTarget);
  }

  return null;
}

function chooseDraftPollingSeedUrl(draft: EnrichStateResourcesInput["draft"]): string | null {
  const stateSpecificFallback = CURATED_STATE_POLLING_URL_BY_FIPS[draft.state_fips];
  if (stateSpecificFallback) {
    const normalizedFallback = normalizeHttpUrl(stateSpecificFallback);
    if (normalizedFallback) {
      return normalizedFallback;
    }
  }

  for (const seed of draft.seed_sources) {
    const normalized = normalizeHttpUrl(seed);
    if (!normalized) {
      continue;
    }
    if (isLikelyPollingPlaceUrlByUrl(normalized)) {
      return normalized;
    }
  }
  return null;
}

function isCuratedStatePollingUrl(url: string, draft: EnrichStateResourcesInput["draft"]): boolean {
  const curated = CURATED_STATE_POLLING_URL_BY_FIPS[draft.state_fips];
  if (!curated) {
    return false;
  }

  const normalizedCurated = normalizeHttpUrl(curated);
  const normalizedUrl = normalizeHttpUrl(url);
  return typeof normalizedCurated === "string" && normalizedCurated === normalizedUrl;
}

/**
 * Normalizes AI citations and deduplicates URL entries by normalized source_url.
 * Applies deterministic fallbacks only for URL fields (not legal summary text fields).
 */
function groundCitationsToEvidence(
  payload: StateResourcePayload,
  evidence: EnrichStateResourcesInput["evidence"]
): StateResourcePayload {
  const groundedSources = {} as StateResourcePayload["sources"];

  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const seen = new Set<string>();
    const grounded = payload.sources[key]
      .map((citation) => {
        const normalized = normalizeHttpUrl(citation.source_url);
        if (!normalized || seen.has(normalized)) {
          return null;
        }
        seen.add(normalized);
        return {
          source_name: citation.source_name.trim().length > 0 ? citation.source_name.trim() : getHostname(normalized),
          source_url: normalized,
        };
      })
      .filter((citation): citation is { source_name: string; source_url: string } => citation !== null);

    if (grounded.length === 0 && !isLegalSummaryCitationField(key)) {
      const fallback = chooseFallbackEvidenceUrl(key, evidence);
      if (fallback) {
        grounded.push({
          source_name: fallback.sourceName,
          source_url: fallback.url,
        });
        seen.add(fallback.url);
      }
    }

    if (isPreferredOfficialCitationField(key)) {
      const hasOfficialCitation = grounded.some((citation) =>
        isOfficialElectionSource(citation.source_url, citation.source_name)
      );

      if (!hasOfficialCitation) {
        const preferredOfficial = choosePreferredOfficialCitationForField(key, evidence);
        if (preferredOfficial && !seen.has(preferredOfficial.url)) {
          grounded.unshift({
            source_name: preferredOfficial.sourceName,
            source_url: preferredOfficial.url,
          });
          seen.add(preferredOfficial.url);
        }
      }
    }

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

  // Deterministic identity fields must match draft input (do not let AI alter them).
  if (parsed.payload.state_fips !== expectedStateFips) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "state_fips in AI output must match draft state_fips",
      failureDebug: providerFailureDebug,
    };
  }

  if (parsed.payload.state_abbreviation !== expectedStateAbbreviation) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "state_abbreviation in AI output must match draft state_abbreviation",
      failureDebug: providerFailureDebug,
    };
  }

  if (parsed.payload.state_name !== expectedStateName) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "state_name in AI output must match draft state_name",
      failureDebug: providerFailureDebug,
    };
  }

  const pollingNormalizedPayload = preferOfficialPollingPlaceUrl(parsed.payload, input.evidence, input.draft);
  let normalizedPayload = groundCitationsToEvidence(pollingNormalizedPayload, input.evidence);
  const pollingSeedFallback = chooseDraftPollingSeedUrl(input.draft);
  if (pollingSeedFallback) {
    const normalizedCurrentPollingUrl = normalizeHttpUrl(normalizedPayload.polling_place_url);
    if (
      (
        !normalizedCurrentPollingUrl ||
        !isLikelyPollingPlaceUrlByUrl(normalizedCurrentPollingUrl) ||
        isAggregatorUrl(normalizedCurrentPollingUrl)
      )
    ) {
      const fallbackChangedPollingUrl = normalizedCurrentPollingUrl !== pollingSeedFallback;

      if (fallbackChangedPollingUrl) {
        const fallbackSourceName =
          getEvidenceSourceNameForUrl(pollingSeedFallback, input.evidence) ?? getHostname(pollingSeedFallback);
        normalizedPayload = {
          ...normalizedPayload,
          polling_place_url: pollingSeedFallback,
          sources: {
            ...normalizedPayload.sources,
            polling_place_url: [
              {
                source_name: fallbackSourceName,
                source_url: pollingSeedFallback,
              },
            ],
          },
        };
      } else {
        normalizedPayload = {
          ...normalizedPayload,
          polling_place_url: pollingSeedFallback,
        };
      }
    }
  }

  if (
    !isLikelyPollingPlaceUrlByUrl(normalizedPayload.polling_place_url) &&
    !isCuratedStatePollingUrl(normalizedPayload.polling_place_url, input.draft)
  ) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "polling_place_url must be a polling-place locator URL, not a registration/mail/id URL",
      failureDebug: providerFailureDebug,
    };
  }

  for (const field of LEGAL_SUMMARY_CITATION_FIELDS) {
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
