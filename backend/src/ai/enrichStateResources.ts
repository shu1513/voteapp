import {
  STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
  STATE_RESOURCE_SOURCE_FIELDS,
} from "../contracts/stateResourceEnrichmentContract.js";
import type { PipelineEnv } from "../config/env.js";
import type {
  AiProvider,
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

const STATE_POLLING_FALLBACK_BY_FIPS: Record<string, string> = {
  "01": "https://myinfo.alabamavotes.gov/voterview",
  "02": "https://myvoterportal.alaska.gov/",
  "04": "https://my.arizona.vote/WhereToVote.aspx?s=address",
  "09": "https://portaldir.ct.gov/sots/LookUp.aspx",
  "10": "https://ivote.de.gov/VoterView",
  "18": "https://indianavoters.in.gov/",
  "23": "https://www.maine.gov/portal/government/edemocracy/voter_lookup.php",
  "25": "https://www.sec.state.ma.us/wheredoivotema/bal/MyElectionInfo.aspx",
  "30": "https://app.mt.gov/voterinfo/",
  "49": "https://votesearch.utah.gov/voter-search/search/search-by-address/how-and-where-can-i-vote",
};

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

function getPathname(url: string): string {
  try {
    return new URL(url).pathname.toLowerCase();
  } catch {
    return "";
  }
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

  if (
    titleLower.includes(stateNameLower) ||
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
): { ok: true; urlSet: Set<string> } | { ok: false; reason: string } {
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

  return { ok: true, urlSet };
}

/**
 * Ensures every citation URL is grounded in the retrieved evidence set.
 */
function validateCitationsFromEvidence(payload: StateResourcePayload, evidenceUrlSet: Set<string>): string | null {
  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    for (const citation of payload.sources[key]) {
      const normalizedCitationUrl = normalizeHttpUrl(citation.source_url);
      if (!normalizedCitationUrl || !evidenceUrlSet.has(normalizedCitationUrl)) {
        return `sources.${key} citation URL must come from collected evidence URLs`;
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
    .filter((item): item is { url: string; sourceName: string; score: number } => item !== null)
    .sort((a, b) => b.score - a.score);

  if (ranked.length === 0) {
    return null;
  }

  return {
    url: ranked[0].url,
    sourceName: ranked[0].sourceName,
  };
}

function chooseDraftPollingSeedUrl(draft: EnrichStateResourcesInput["draft"]): string | null {
  const stateSpecificFallback = STATE_POLLING_FALLBACK_BY_FIPS[draft.state_fips];
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
  const curated = STATE_POLLING_FALLBACK_BY_FIPS[draft.state_fips];
  if (!curated) {
    return false;
  }

  const normalizedCurated = normalizeHttpUrl(curated);
  const normalizedUrl = normalizeHttpUrl(url);
  return typeof normalizedCurated === "string" && normalizedCurated === normalizedUrl;
}

/**
 * Normalizes AI citations to URLs that are actually present in collected evidence.
 * Keeps existing in-evidence citations; fills missing buckets with deterministic evidence fallbacks.
 */
function groundCitationsToEvidence(
  payload: StateResourcePayload,
  evidence: EnrichStateResourcesInput["evidence"],
  evidenceUrlSet: Set<string>
): StateResourcePayload {
  const groundedSources = {} as StateResourcePayload["sources"];

  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const seen = new Set<string>();
    const grounded = payload.sources[key]
      .map((citation) => {
        const normalized = normalizeHttpUrl(citation.source_url);
        if (!normalized || !evidenceUrlSet.has(normalized) || seen.has(normalized)) {
          return null;
        }
        seen.add(normalized);
        return {
          source_name: citation.source_name.trim().length > 0 ? citation.source_name.trim() : getHostname(normalized),
          source_url: normalized,
        };
      })
      .filter((citation): citation is { source_name: string; source_url: string } => citation !== null);

    if (grounded.length === 0) {
      const fallback = chooseFallbackEvidenceUrl(key, evidence);
      if (fallback) {
        grounded.push({
          source_name: fallback.sourceName,
          source_url: fallback.url,
        });
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

  const parsed = parseStateResourcePayloadFromAi(generated.rawPayload);
  if (!parsed.ok) {
    return {
      ok: false,
      retryable: false,
      errorCode: parsed.errorCode,
      reason: parsed.reason,
    };
  }

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
    };
  }

  if (parsed.payload.state_abbreviation !== expectedStateAbbreviation) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "state_abbreviation in AI output must match draft state_abbreviation",
    };
  }

  if (parsed.payload.state_name !== expectedStateName) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "state_name in AI output must match draft state_name",
    };
  }

  const pollingNormalizedPayload = preferOfficialPollingPlaceUrl(parsed.payload, input.evidence, input.draft);
  let normalizedPayload = groundCitationsToEvidence(pollingNormalizedPayload, input.evidence, evidenceCheck.urlSet);
  const pollingSeedFallback = chooseDraftPollingSeedUrl(input.draft);
  if (pollingSeedFallback) {
    const normalizedCurrentPollingUrl = normalizeHttpUrl(normalizedPayload.polling_place_url);
    if (
      !normalizedCurrentPollingUrl ||
      !isLikelyPollingPlaceUrlByUrl(normalizedCurrentPollingUrl) ||
      isAggregatorUrl(normalizedCurrentPollingUrl)
    ) {
      normalizedPayload = {
        ...normalizedPayload,
        polling_place_url: pollingSeedFallback,
      };
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
    };
  }

  const evidenceReason = validateCitationsFromEvidence(normalizedPayload, evidenceCheck.urlSet);
  if (evidenceReason) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: evidenceReason,
    };
  }

  return {
    ok: true,
    payload: normalizedPayload,
    schemaVersion: STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
    provider: config.provider,
    model: config.model,
    promptVersion: input.promptVersion,
  };
}
