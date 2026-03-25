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

const PROVIDER_ADAPTERS: Record<AiProvider, ProviderAdapter> = {
  openai: openAiProvider,
  claude: claudeProvider,
  gemini: geminiProvider,
};

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

  const evidenceReason = validateCitationsFromEvidence(parsed.payload, evidenceCheck.urlSet);
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
    payload: parsed.payload,
    schemaVersion: STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
    provider: config.provider,
    model: config.model,
    promptVersion: input.promptVersion,
  };
}
