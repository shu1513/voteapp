import { ELECTIONS_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import { buildElectionsPrompt } from "./providers/electionsPrompt.js";
import {
  extractProviderRateLimitDebugHeaders,
  updateProviderModelCooldownFromHeaders,
  waitForProviderModelCooldown,
} from "./providerRateLimitGate.js";
import {
  ELECTION_ENRICHMENT_SCHEMA_VERSION,
} from "../contracts/electionEnrichmentContract.js";
import { parseAiElectionEntriesPayload } from "../contracts/electionPayloadContract.js";
import type { AiProvider } from "./types.js";
import type { ElectionDraftPayload, ElectionEnrichedPayload } from "../types/election.js";

const OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses";
const ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages";

type RetryableErrorCode = "RATE_LIMIT" | "TIMEOUT" | "TEMP_PROVIDER_ERROR";
type PermanentErrorCode = "INVALID_JSON" | "SCHEMA_MISMATCH" | "MISSING_REQUIRED_FIELDS" | "CONFIGURATION_ERROR";

type ElectionEnrichmentFailure = {
  ok: false;
  retryable: boolean;
  errorCode: RetryableErrorCode | PermanentErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ElectionEnrichmentSuccess = {
  ok: true;
  payload: ElectionEnrichedPayload;
  provider: AiProvider;
  model: string;
  schemaVersion: typeof ELECTION_ENRICHMENT_SCHEMA_VERSION;
  promptVersion: string;
  aiRawDebug: Record<string, unknown> | null;
};

export type EnrichElectionsResult = ElectionEnrichmentSuccess | ElectionEnrichmentFailure;

export type EnrichElectionsInput = {
  ingestKey: string;
  draft: ElectionDraftPayload;
  promptVersion: string;
  softRetryCount: number;
  reviewFeedback: string[];
};

export type EnrichElectionsConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function trimDebugText(input: string, maxChars = 20_000): string {
  return input.length <= maxChars ? input : `${input.slice(0, maxChars)}...`;
}

function extractJsonCandidate(text: string): string {
  const trimmed = text.trim();

  const fenced = /```(?:json)?\s*([\s\S]*?)\s*```/i.exec(trimmed);
  if (fenced?.[1]) {
    return fenced[1].trim();
  }

  return trimmed;
}

function extractResponsesOutputText(responsePayload: unknown): string | null {
  if (typeof responsePayload !== "object" || responsePayload === null) {
    return null;
  }
  const input = responsePayload as Record<string, unknown>;

  if (typeof input.output_text === "string" && input.output_text.trim().length > 0) {
    return input.output_text;
  }

  const output = input.output;
  if (!Array.isArray(output)) {
    return null;
  }

  const parts: string[] = [];
  for (const item of output) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      continue;
    }
    const outputItem = item as Record<string, unknown>;
    if (outputItem.type !== "message") {
      continue;
    }

    const content = outputItem.content;
    if (!Array.isArray(content)) {
      continue;
    }

    for (const contentPart of content) {
      if (typeof contentPart !== "object" || contentPart === null || Array.isArray(contentPart)) {
        continue;
      }
      const outputPart = contentPart as Record<string, unknown>;
      if (outputPart.type !== "output_text" || typeof outputPart.text !== "string") {
        continue;
      }
      const text = outputPart.text.trim();
      if (text.length > 0) {
        parts.push(text);
      }
    }
  }

  if (parts.length === 0) {
    return null;
  }
  return parts.join("\n");
}

function extractOpenAiWebSearchSources(responsePayload: unknown): Array<{ url?: string; title?: string }> {
  if (typeof responsePayload !== "object" || responsePayload === null) {
    return [];
  }
  const input = responsePayload as Record<string, unknown>;
  const output = input.output;
  if (!Array.isArray(output)) {
    return [];
  }

  const sources: Array<{ url?: string; title?: string }> = [];
  for (const item of output) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      continue;
    }
    const outputItem = item as Record<string, unknown>;
    if (outputItem.type !== "web_search_call") {
      continue;
    }
    const action = outputItem.action;
    if (typeof action !== "object" || action === null || Array.isArray(action)) {
      continue;
    }
    const actionRecord = action as Record<string, unknown>;
    const rawSources = actionRecord.sources;
    if (!Array.isArray(rawSources)) {
      continue;
    }
    for (const source of rawSources) {
      if (typeof source !== "object" || source === null || Array.isArray(source)) {
        continue;
      }
      const sourceRecord = source as Record<string, unknown>;
      sources.push({
        ...(typeof sourceRecord.url === "string" ? { url: sourceRecord.url } : {}),
        ...(typeof sourceRecord.title === "string" ? { title: sourceRecord.title } : {}),
      });
    }
  }

  return sources;
}

function shouldSetExplicitTemperature(model: string): boolean {
  return !model.toLowerCase().startsWith("gpt-5");
}

async function callOpenAiResponsesWithWebSearch(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<
  | { ok: true; parsed: unknown; rawText: string; responsesDebug?: Record<string, unknown> }
  | ElectionEnrichmentFailure
> {
  let controller: AbortController | null = null;
  let timeout: ReturnType<typeof setTimeout> | null = null;

  try {
    await waitForProviderModelCooldown("openai", model);
    const requestController = new AbortController();
    controller = requestController;
    timeout = setTimeout(() => requestController.abort(), timeoutMs);

    const requestBody: Record<string, unknown> = {
      model,
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: "Return strict JSON only." }],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: prompt }],
        },
      ],
      tools: [{ type: "web_search" }],
      tool_choice: "auto",
      include: ["web_search_call.action.sources"],
    };
    if (shouldSetExplicitTemperature(model)) {
      requestBody.temperature = 0;
    }

    const response = await fetch(OPENAI_RESPONSES_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
      signal: controller!.signal,
    });

    if (!response.ok) {
      const rateLimitHeaders = extractProviderRateLimitDebugHeaders(response.headers);
      updateProviderModelCooldownFromHeaders("openai", model, response.headers, {
        onRateLimitedResponse: response.status === 429,
      });
      const bodyText = await response.text();
      if (response.status === 429) {
        return {
          ok: false,
          retryable: true,
          errorCode: "RATE_LIMIT",
          reason: `OpenAI responses rate limit: ${bodyText}`,
          failureDebug: {
            provider_response_text: trimDebugText(bodyText),
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      if (response.status >= 500) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TEMP_PROVIDER_ERROR",
          reason: `OpenAI responses temporary error ${response.status}: ${bodyText}`,
          failureDebug: {
            provider_response_text: trimDebugText(bodyText),
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `OpenAI responses request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          provider_response_text: trimDebugText(bodyText),
          provider_rate_limit_headers: rateLimitHeaders,
        },
      };
    }

    updateProviderModelCooldownFromHeaders("openai", model, response.headers);

    const data = (await response.json()) as Record<string, unknown>;
    const text = extractResponsesOutputText(data);
    if (!text || text.trim().length === 0) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "OpenAI responses returned empty assistant text",
        failureDebug: {
          provider_response_payload: data,
        },
      };
    }

    const webSearchSources = extractOpenAiWebSearchSources(data);
    try {
      return {
        ok: true,
        parsed: JSON.parse(extractJsonCandidate(text)),
        rawText: text,
        responsesDebug: {
          web_search_sources: webSearchSources,
          web_search_sources_count: webSearchSources.length,
        },
      };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `OpenAI responses returned invalid JSON: ${toReason(error)}`,
        failureDebug: {
          provider_response_text: trimDebugText(text),
          web_search_sources: webSearchSources,
          web_search_sources_count: webSearchSources.length,
        },
      };
    }
  } catch (error) {
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return {
        ok: false,
        retryable: true,
        errorCode: "TIMEOUT",
        reason: `OpenAI responses request timed out after ${timeoutMs}ms`,
      };
    }
    return {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: `OpenAI responses request error: ${reason}`,
    };
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

async function callClaude(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number,
  webSearchMaxUses = 3
): Promise<{ ok: true; parsed: unknown; rawText: string } | ElectionEnrichmentFailure> {
  let controller: AbortController | null = null;
  let timeout: ReturnType<typeof setTimeout> | null = null;

  try {
    await waitForProviderModelCooldown("claude", model);
    const requestController = new AbortController();
    controller = requestController;
    timeout = setTimeout(() => requestController.abort(), timeoutMs);

    const requestBody: Record<string, unknown> = {
      model,
      max_tokens: 4000,
      temperature: 0,
      system: "Return strict JSON only.",
      messages: [{ role: "user", content: prompt }],
    };
    const headers: Record<string, string> = {
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    };
    requestBody.tools = [
      {
        type: "web_search_20250305",
        name: "web_search",
        max_uses: Math.max(1, Math.floor(webSearchMaxUses)),
      },
    ];
    headers["anthropic-beta"] = "web-search-2025-03-05";

    const response = await fetch(ANTHROPIC_MESSAGES_URL, {
      method: "POST",
      headers,
      body: JSON.stringify(requestBody),
      signal: controller!.signal,
    });

    if (!response.ok) {
      const rateLimitHeaders = extractProviderRateLimitDebugHeaders(response.headers);
      updateProviderModelCooldownFromHeaders("claude", model, response.headers, {
        onRateLimitedResponse: response.status === 429,
      });
      const bodyText = await response.text();
      if (response.status === 429) {
        return {
          ok: false,
          retryable: true,
          errorCode: "RATE_LIMIT",
          reason: `Claude rate limit: ${bodyText}`,
          failureDebug: {
            provider_response_text: trimDebugText(bodyText),
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      if (response.status >= 500) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TEMP_PROVIDER_ERROR",
          reason: `Claude temporary error ${response.status}: ${bodyText}`,
          failureDebug: {
            provider_response_text: trimDebugText(bodyText),
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `Claude request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          provider_response_text: trimDebugText(bodyText),
          provider_rate_limit_headers: rateLimitHeaders,
        },
      };
    }

    updateProviderModelCooldownFromHeaders("claude", model, response.headers);

    const data = (await response.json()) as {
      content?: Array<{ type?: string; text?: string }>;
    };
    const text = data.content?.find((part) => part.type === "text")?.text;
    if (!text || text.trim().length === 0) {
      return { ok: false, retryable: false, errorCode: "INVALID_JSON", reason: "Claude returned empty content" };
    }
    try {
      return { ok: true, parsed: JSON.parse(extractJsonCandidate(text)), rawText: text };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `Claude returned invalid JSON: ${toReason(error)}`,
        failureDebug: { provider_response_text: trimDebugText(text) },
      };
    }
  } catch (error) {
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return { ok: false, retryable: true, errorCode: "TIMEOUT", reason: `Claude request timed out after ${timeoutMs}ms` };
    }
    return { ok: false, retryable: true, errorCode: "TEMP_PROVIDER_ERROR", reason: `Claude request error: ${reason}` };
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

async function callGemini(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string } | ElectionEnrichmentFailure> {
  let controller: AbortController | null = null;
  let timeout: ReturnType<typeof setTimeout> | null = null;
  const url = `https://generativelanguage.googleapis.com/v1/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;

  try {
    await waitForProviderModelCooldown("gemini", model);
    const requestController = new AbortController();
    controller = requestController;
    timeout = setTimeout(() => requestController.abort(), timeoutMs);

    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        generationConfig: { temperature: 0 },
        contents: [{ role: "user", parts: [{ text: prompt }] }],
      }),
      signal: controller!.signal,
    });

    if (!response.ok) {
      const rateLimitHeaders = extractProviderRateLimitDebugHeaders(response.headers);
      updateProviderModelCooldownFromHeaders("gemini", model, response.headers, {
        onRateLimitedResponse: response.status === 429,
      });
      const bodyText = await response.text();
      if (response.status === 429) {
        return {
          ok: false,
          retryable: true,
          errorCode: "RATE_LIMIT",
          reason: `Gemini rate limit: ${bodyText}`,
          failureDebug: {
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      if (response.status >= 500) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TEMP_PROVIDER_ERROR",
          reason: `Gemini temporary error ${response.status}: ${bodyText}`,
          failureDebug: {
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `Gemini request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          provider_rate_limit_headers: rateLimitHeaders,
        },
      };
    }

    updateProviderModelCooldownFromHeaders("gemini", model, response.headers);

    const data = (await response.json()) as {
      candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }>;
    };
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!text || text.trim().length === 0) {
      return { ok: false, retryable: false, errorCode: "INVALID_JSON", reason: "Gemini returned empty content" };
    }
    try {
      return { ok: true, parsed: JSON.parse(extractJsonCandidate(text)), rawText: text };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `Gemini returned invalid JSON: ${toReason(error)}`,
        failureDebug: { provider_response_text: trimDebugText(text) },
      };
    }
  } catch (error) {
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return { ok: false, retryable: true, errorCode: "TIMEOUT", reason: `Gemini request timed out after ${timeoutMs}ms` };
    }
    return { ok: false, retryable: true, errorCode: "TEMP_PROVIDER_ERROR", reason: `Gemini request error: ${reason}` };
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

async function callOpenAi(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | ElectionEnrichmentFailure> {
  const responsesResult = await callOpenAiResponsesWithWebSearch(
    prompt,
    model,
    apiKey,
    timeoutMs
  );
  if (responsesResult.ok) {
    return {
      ok: true,
      parsed: responsesResult.parsed,
      rawText: responsesResult.rawText,
      debugMeta: {
        openai_api_mode: "responses_web_search",
        ...(responsesResult.responsesDebug ?? {}),
      },
    };
  }
  return responsesResult;
}

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: EnrichElectionsConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | ElectionEnrichmentFailure> {
  if (candidate.provider === "openai") {
    if (!config.openAiApiKey) {
      return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "OPENAI_API_KEY is missing" };
    }
    return callOpenAi(prompt, candidate.model, config.openAiApiKey, config.timeoutMs);
  }
    if (candidate.provider === "claude") {
      if (!config.anthropicApiKey) {
        return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "ANTHROPIC_API_KEY is missing" };
      }
      return callClaude(
        prompt,
        candidate.model,
        config.anthropicApiKey,
        config.timeoutMs,
        config.anthropicWebSearchMaxUses
      );
    }
  if (!config.geminiApiKey) {
    return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "GEMINI_API_KEY is missing" };
  }
  return callGemini(prompt, candidate.model, config.geminiApiKey, config.timeoutMs);
}

export function buildEnrichElectionsConfigFromEnv(): EnrichElectionsConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichElections(
  input: EnrichElectionsInput,
  config: EnrichElectionsConfig,
  candidates: readonly AiCandidate[] = ELECTIONS_AI_CANDIDATES
): Promise<EnrichElectionsResult> {
  const prompt = buildElectionsPrompt({
    draft: input.draft,
    softRetryCount: input.softRetryCount,
    reviewFeedbackLines: input.reviewFeedback,
  });

  const failures: Array<{
    provider: string;
    model: string;
    reason: string;
    errorCode: string;
    retryable: boolean;
    failureDebug?: Record<string, unknown>;
  }> = [];

  for (const candidate of candidates) {
    const generated = await callProvider(candidate, prompt, config);
    if (!generated.ok) {
      failures.push({
        provider: candidate.provider,
        model: candidate.model,
        reason: generated.reason,
        errorCode: generated.errorCode,
        retryable: generated.retryable,
        failureDebug: generated.failureDebug,
      });
      if (!generated.retryable) {
        continue;
      }
      continue;
    }

    const parsed = parseAiElectionEntriesPayload(generated.parsed);
    if (!parsed.ok) {
      failures.push({
        provider: candidate.provider,
        model: candidate.model,
        reason: parsed.reason,
        errorCode: "SCHEMA_MISMATCH",
        retryable: false,
      });
      continue;
    }

    const canonicalPayload: ElectionEnrichedPayload = {
      district_id: input.draft.district_id,
      district_name: input.draft.district_name,
      district_type: input.draft.district_type,
      state: input.draft.state,
      entries: parsed.payload.entries,
      ...(parsed.payload.review_decision ? { review_decision: parsed.payload.review_decision } : {}),
      ...(parsed.payload.review_reason ? { review_reason: parsed.payload.review_reason } : {}),
    };

    return {
      ok: true,
      payload: canonicalPayload,
      provider: candidate.provider,
      model: candidate.model,
      schemaVersion: ELECTION_ENRICHMENT_SCHEMA_VERSION,
      promptVersion: input.promptVersion,
      aiRawDebug: {
        provider_response_text: trimDebugText(generated.rawText),
        ...(generated.debugMeta ?? {}),
      },
    };
  }

  const finalFailure = failures[failures.length - 1];
  const anyRetryable = failures.some((failure) => failure.retryable);
  const firstPermanentFailure = failures.find((failure) => !failure.retryable && failure.errorCode);
  const firstRetryableFailure = failures.find((failure) => failure.retryable && failure.errorCode);
  const selectedFailure = anyRetryable
    ? (firstRetryableFailure ?? finalFailure)
    : (firstPermanentFailure ?? finalFailure);

  return {
    ok: false,
    retryable: selectedFailure?.retryable ?? false,
    errorCode:
      (selectedFailure?.errorCode as RetryableErrorCode | PermanentErrorCode | undefined) ??
      "TEMP_PROVIDER_ERROR",
    reason: selectedFailure?.reason ?? "No AI candidates available for election enrichment",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(prompt, 6000),
    },
  };
}
