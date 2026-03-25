import type {
  StateResourceDraftPayload,
  StateResourcePayload,
} from "../types/stateResource.js";

export type AiProvider = "openai" | "claude" | "gemini";

export type EvidenceSnippet = {
  url: string;
  title: string;
  snippet: string;
};

export type EnrichStateResourcesInput = {
  ingestKey: string;
  draft: StateResourceDraftPayload;
  evidence: EvidenceSnippet[];
  promptVersion: string;
};

export type EnrichStateResourcesConfig = {
  provider: AiProvider;
  model: string;
  timeoutMs: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type RetryableErrorCode =
  | "RATE_LIMIT"
  | "TIMEOUT"
  | "TEMP_PROVIDER_ERROR";

export type PermanentErrorCode =
  | "INVALID_JSON"
  | "SCHEMA_MISMATCH"
  | "MISSING_REQUIRED_FIELDS"
  | "CONFIGURATION_ERROR"
  | "UNSUPPORTED_PROVIDER";

export type EnrichmentFailure =
  | {
      ok: false;
      retryable: true;
      reason: string;
      errorCode: RetryableErrorCode;
    }
  | {
      ok: false;
      retryable: false;
      reason: string;
      errorCode: PermanentErrorCode;
    };

export type EnrichmentSuccess = {
  ok: true;
  payload: StateResourcePayload;
  schemaVersion: "state_resources_enrichment_v1";
  provider: AiProvider;
  model: string;
  promptVersion: string;
};

export type EnrichStateResourcesResult = EnrichmentSuccess | EnrichmentFailure;

export type ProviderGenerateResult =
  | { ok: true; rawPayload: unknown }
  | EnrichmentFailure;

export type ProviderAdapter = (
  input: EnrichStateResourcesInput,
  config: EnrichStateResourcesConfig
) => Promise<ProviderGenerateResult>;
