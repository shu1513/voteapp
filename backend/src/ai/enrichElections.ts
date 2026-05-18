import { ELECTIONS_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import { buildElectionsPrompt } from "./providers/electionsPrompt.js";
import {
  ELECTION_ENRICHMENT_SCHEMA_VERSION,
} from "../contracts/electionEnrichmentContract.js";
import { parseCanonicalElectionPayload } from "../contracts/electionPayloadContract.js";
import type { AiProvider } from "./types.js";
import type { ElectionDraftPayload, ElectionEnrichedPayload } from "../types/election.js";

const OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions";
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

function shouldSetExplicitTemperature(model: string): boolean {
  return !model.toLowerCase().startsWith("gpt-5");
}

async function callOpenAi(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string } | ElectionEnrichmentFailure> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const requestBody: Record<string, unknown> = {
      model,
      messages: [
        { role: "system", content: "Return strict JSON only." },
        { role: "user", content: prompt },
      ],
      response_format: { type: "json_object" },
    };
    if (shouldSetExplicitTemperature(model)) {
      requestBody.temperature = 0;
    }

    const response = await fetch(OPENAI_CHAT_COMPLETIONS_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal,
    });

    if (!response.ok) {
      const bodyText = await response.text();
      if (response.status === 429) {
        return { ok: false, retryable: true, errorCode: "RATE_LIMIT", reason: `OpenAI rate limit: ${bodyText}` };
      }
      if (response.status >= 500) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TEMP_PROVIDER_ERROR",
          reason: `OpenAI temporary error ${response.status}: ${bodyText}`,
        };
      }
      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `OpenAI request failed ${response.status}: ${bodyText}`,
      };
    }

    const data = (await response.json()) as {
      choices?: Array<{ message?: { content?: string } }>;
    };
    const text = data.choices?.[0]?.message?.content;
    if (!text || text.trim().length === 0) {
      return { ok: false, retryable: false, errorCode: "INVALID_JSON", reason: "OpenAI returned empty content" };
    }

    try {
      return { ok: true, parsed: JSON.parse(extractJsonCandidate(text)), rawText: text };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `OpenAI returned invalid JSON: ${toReason(error)}`,
        failureDebug: { provider_response_text: trimDebugText(text) },
      };
    }
  } catch (error) {
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return { ok: false, retryable: true, errorCode: "TIMEOUT", reason: `OpenAI request timed out after ${timeoutMs}ms` };
    }
    return { ok: false, retryable: true, errorCode: "TEMP_PROVIDER_ERROR", reason: `OpenAI request error: ${reason}` };
  } finally {
    clearTimeout(timeout);
  }
}

async function callClaude(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string } | ElectionEnrichmentFailure> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(ANTHROPIC_MESSAGES_URL, {
      method: "POST",
      headers: {
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model,
        max_tokens: 4000,
        temperature: 0,
        system: "Return strict JSON only.",
        messages: [{ role: "user", content: prompt }],
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const bodyText = await response.text();
      if (response.status === 429) {
        return { ok: false, retryable: true, errorCode: "RATE_LIMIT", reason: `Claude rate limit: ${bodyText}` };
      }
      if (response.status >= 500) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TEMP_PROVIDER_ERROR",
          reason: `Claude temporary error ${response.status}: ${bodyText}`,
        };
      }
      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `Claude request failed ${response.status}: ${bodyText}`,
      };
    }

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
    clearTimeout(timeout);
  }
}

async function callGemini(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string } | ElectionEnrichmentFailure> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const url = `https://generativelanguage.googleapis.com/v1/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        generationConfig: { temperature: 0 },
        contents: [{ role: "user", parts: [{ text: prompt }] }],
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const bodyText = await response.text();
      if (response.status === 429) {
        return { ok: false, retryable: true, errorCode: "RATE_LIMIT", reason: `Gemini rate limit: ${bodyText}` };
      }
      if (response.status >= 500) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TEMP_PROVIDER_ERROR",
          reason: `Gemini temporary error ${response.status}: ${bodyText}`,
        };
      }
      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `Gemini request failed ${response.status}: ${bodyText}`,
      };
    }

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
    clearTimeout(timeout);
  }
}

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: EnrichElectionsConfig
): Promise<{ ok: true; parsed: unknown; rawText: string } | ElectionEnrichmentFailure> {
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
    return callClaude(prompt, candidate.model, config.anthropicApiKey, config.timeoutMs);
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
      });
      if (!generated.retryable) {
        continue;
      }
      continue;
    }

    const parsed = parseCanonicalElectionPayload(generated.parsed);
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

    return {
      ok: true,
      payload: parsed.payload,
      provider: candidate.provider,
      model: candidate.model,
      schemaVersion: ELECTION_ENRICHMENT_SCHEMA_VERSION,
      promptVersion: input.promptVersion,
      aiRawDebug: {
        provider_response_text: trimDebugText(generated.rawText),
      },
    };
  }

  const finalFailure = failures[failures.length - 1];
  const anyRetryable = failures.some((failure) => failure.retryable);
  const firstPermanentFailure = failures.find((failure) => !failure.retryable && failure.errorCode);
  const firstRetryableFailure = failures.find((failure) => failure.retryable && failure.errorCode);
  const selectedErrorCode =
    firstPermanentFailure?.errorCode ??
    firstRetryableFailure?.errorCode ??
    "TEMP_PROVIDER_ERROR";

  return {
    ok: false,
    retryable: anyRetryable,
    errorCode: selectedErrorCode as RetryableErrorCode | PermanentErrorCode,
    reason: finalFailure?.reason ?? "No AI candidates available for election enrichment",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(prompt, 6000),
    },
  };
}
