import type { AiProvider } from "./types.js";
import type { AiCandidate } from "./aiCandidates.js";
import { AI_CALLS_BLOCKED_REASON, isAiApiCallAllowed } from "./aiCallGuard.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";
import {
  extractProviderRateLimitDebugHeaders,
  updateProviderModelCooldownFromHeaders,
  waitForProviderModelCooldown,
} from "./providerRateLimitGate.js";

const OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses";
const ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages";

export type ResearchRetryableErrorCode = "RATE_LIMIT" | "TIMEOUT" | "TEMP_PROVIDER_ERROR";
export type ResearchPermanentErrorCode = "INVALID_JSON" | "CONFIGURATION_ERROR";
export type ResearchErrorCode = ResearchRetryableErrorCode | ResearchPermanentErrorCode;

export type ResearchProviderFailure = {
  ok: false;
  retryable: boolean;
  errorCode: ResearchErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

export type ResearchProviderSuccess = {
  ok: true;
  parsed: unknown;
  rawText: string;
  debugMeta?: Record<string, unknown>;
};

export type ResearchProviderResult = ResearchProviderSuccess | ResearchProviderFailure;

export type ResearchProviderConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
  claudeInterCallDelayMs?: number;
  claudeRetryAfterBufferMs?: number;
  geminiApiVersion?: "v1" | "v1beta";
  geminiResponseMimeTypeJson?: boolean;
};

type ClaudeLaneState = {
  lane: Promise<void>;
  lastStartedAt: number;
};

const claudeLaneByKey = new Map<string, ClaudeLaneState>();

function getClaudeLaneState(key: string): ClaudeLaneState {
  const existing = claudeLaneByKey.get(key);
  if (existing) {
    return existing;
  }
  const created: ClaudeLaneState = {
    lane: Promise.resolve(),
    lastStartedAt: 0,
  };
  claudeLaneByKey.set(key, created);
  return created;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runInClaudeLane<T>(
  laneKey: string,
  minDelayMs: number,
  work: () => Promise<T>
): Promise<T> {
  const state = getClaudeLaneState(laneKey);
  const run = state.lane.catch(() => undefined).then(async () => {
    const nextAllowedStart = state.lastStartedAt + minDelayMs;
    const waitMs = nextAllowedStart - Date.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }
    state.lastStartedAt = Date.now();
    return work();
  });
  state.lane = run.then(() => undefined, () => undefined);
  return run;
}

export function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function isAbortError(error: unknown): boolean {
  if (typeof error === "object" && error !== null && "name" in error) {
    const name = (error as { name?: unknown }).name;
    if (name === "AbortError") {
      return true;
    }
  }

  // Fallback for environments that do not preserve AbortError.name consistently.
  const reason = toReason(error).toLowerCase();
  return reason.includes("aborted");
}

export function trimDebugText(input: string, maxChars = 20_000): string {
  return input.length <= maxChars ? input : `${input.slice(0, maxChars)}...`;
}

export function extractJsonCandidate(text: string): string {
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

  return parts.length > 0 ? parts.join("\n") : null;
}

function extractOpenAiWebSearchMetadata(responsePayload: unknown): {
  urls: string[];
  sources: Array<{ url?: string; title?: string }>;
} {
  if (typeof responsePayload !== "object" || responsePayload === null) {
    return { urls: [], sources: [] };
  }
  const input = responsePayload as Record<string, unknown>;
  const output = input.output;
  if (!Array.isArray(output)) {
    return { urls: [], sources: [] };
  }

  const urls: string[] = [];
  const sources: Array<{ url?: string; title?: string }> = [];
  const seen = new Set<string>();

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
      const sourceUrl = typeof sourceRecord.url === "string" ? sourceRecord.url : undefined;
      const sourceTitle = typeof sourceRecord.title === "string" ? sourceRecord.title : undefined;
      sources.push({
        ...(sourceUrl ? { url: sourceUrl } : {}),
        ...(sourceTitle ? { title: sourceTitle } : {}),
      });
      if (!sourceUrl) {
        continue;
      }
      const normalized = normalizeHttpUrl(sourceUrl);
      if (!normalized || seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      urls.push(normalized);
    }
  }

  return { urls, sources };
}

function extractClaudeWebSearchUrls(responsePayload: unknown): string[] {
  if (typeof responsePayload !== "object" || responsePayload === null) {
    return [];
  }
  const input = responsePayload as Record<string, unknown>;
  const content = input.content;
  if (!Array.isArray(content)) {
    return [];
  }

  const urls: string[] = [];
  const seen = new Set<string>();
  for (const block of content) {
    if (typeof block !== "object" || block === null || Array.isArray(block)) {
      continue;
    }
    const blockRecord = block as Record<string, unknown>;
    if (blockRecord.type !== "web_search_tool_result") {
      continue;
    }
    const blockContent = blockRecord.content;
    if (!Array.isArray(blockContent)) {
      continue;
    }
    for (const item of blockContent) {
      if (typeof item !== "object" || item === null || Array.isArray(item)) {
        continue;
      }
      const itemRecord = item as Record<string, unknown>;
      if (typeof itemRecord.url !== "string") {
        continue;
      }
      const normalized = normalizeHttpUrl(itemRecord.url);
      if (!normalized || seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      urls.push(normalized);
    }
  }

  return urls;
}

async function callOpenAi(
  prompt: string,
  model: string,
  apiKey: string,
  config: ResearchProviderConfig
): Promise<ResearchProviderResult> {
  let timeout: ReturnType<typeof setTimeout> | null = null;
  try {
    await waitForProviderModelCooldown("openai", model);
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), config.timeoutMs);

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
      signal: controller.signal,
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
      return { ok: false, retryable: false, errorCode: "INVALID_JSON", reason: "OpenAI returned empty assistant text" };
    }

    try {
      const webSearch = extractOpenAiWebSearchMetadata(data);
      return {
        ok: true,
        parsed: JSON.parse(extractJsonCandidate(text)),
        rawText: text,
        debugMeta: {
          openai_api_mode: "responses_web_search",
          web_search_urls: webSearch.urls,
          web_search_urls_count: webSearch.urls.length,
          web_search_sources: webSearch.sources,
          web_search_sources_count: webSearch.sources.length,
        },
      };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `OpenAI returned invalid JSON: ${toReason(error)}`,
        failureDebug: {
          provider_response_text: trimDebugText(text),
        },
      };
    }
  } catch (error) {
    if (isAbortError(error)) {
      return {
        ok: false,
        retryable: true,
        errorCode: "TIMEOUT",
        reason: `OpenAI request timed out after ${config.timeoutMs}ms`,
      };
    }
    const reason = toReason(error);
    return {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: `OpenAI request error: ${reason}`,
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
  config: ResearchProviderConfig
): Promise<ResearchProviderResult> {
  const interCallDelayMs = Math.max(0, config.claudeInterCallDelayMs ?? 0);
  const call = async (): Promise<ResearchProviderResult> => {
    let timeout: ReturnType<typeof setTimeout> | null = null;

    try {
      await waitForProviderModelCooldown("claude", model);
      const controller = new AbortController();
      timeout = setTimeout(() => controller.abort(), config.timeoutMs);

      const requestBody: Record<string, unknown> = {
        model,
        max_tokens: 4000,
        system: "Return strict JSON only.",
        messages: [{ role: "user", content: prompt }],
        tools: [
          {
            type: "web_search_20250305",
            name: "web_search",
            max_uses: Math.max(1, Math.floor(config.anthropicWebSearchMaxUses ?? 3)),
          },
        ],
      };

      const response = await fetch(ANTHROPIC_MESSAGES_URL, {
        method: "POST",
        headers: {
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01",
          "content-type": "application/json",
          "anthropic-beta": "web-search-2025-03-05",
        },
        body: JSON.stringify(requestBody),
        signal: controller.signal,
      });

      if (!response.ok) {
        const rateLimitHeaders = extractProviderRateLimitDebugHeaders(response.headers);
        updateProviderModelCooldownFromHeaders("claude", model, response.headers, {
          onRateLimitedResponse: response.status === 429,
          retryAfterBufferMs: config.claudeRetryAfterBufferMs,
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
        const webSearchUrls = extractClaudeWebSearchUrls(data);
        return {
          ok: true,
          parsed: JSON.parse(extractJsonCandidate(text)),
          rawText: text,
          debugMeta: {
            web_search_urls: webSearchUrls,
            web_search_urls_count: webSearchUrls.length,
          },
        };
      } catch (error) {
        return {
          ok: false,
          retryable: false,
          errorCode: "INVALID_JSON",
          reason: `Claude returned invalid JSON: ${toReason(error)}`,
          failureDebug: {
            provider_response_text: trimDebugText(text),
          },
        };
      }
    } catch (error) {
      if (isAbortError(error)) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TIMEOUT",
          reason: `Claude request timed out after ${config.timeoutMs}ms`,
        };
      }
      const reason = toReason(error);

      return {
        ok: false,
        retryable: true,
        errorCode: "TEMP_PROVIDER_ERROR",
        reason: `Claude request error: ${reason}`,
      };
    } finally {
      if (timeout) {
        clearTimeout(timeout);
      }
    }
  };

  if (interCallDelayMs <= 0) {
    return call();
  }
  return runInClaudeLane("default", interCallDelayMs, call);
}

async function callGemini(
  prompt: string,
  model: string,
  apiKey: string,
  config: ResearchProviderConfig
): Promise<ResearchProviderResult> {
  let timeout: ReturnType<typeof setTimeout> | null = null;
  const apiVersion = config.geminiApiVersion ?? "v1";
  const url = `https://generativelanguage.googleapis.com/${apiVersion}/models/${encodeURIComponent(
    model
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;

  try {
    await waitForProviderModelCooldown("gemini", model);
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), config.timeoutMs);

    const generationConfig: Record<string, unknown> = {};
    if (config.geminiResponseMimeTypeJson) {
      generationConfig.responseMimeType = "application/json";
    }

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        ...(Object.keys(generationConfig).length > 0 ? { generationConfig } : {}),
      }),
      signal: controller.signal,
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
          reason: `Gemini temporary error ${response.status}: ${bodyText}`,
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
        reason: `Gemini request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          provider_response_text: trimDebugText(bodyText),
          provider_rate_limit_headers: rateLimitHeaders,
        },
      };
    }

    updateProviderModelCooldownFromHeaders("gemini", model, response.headers);

    const data = (await response.json()) as {
      candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }>;
    };

    const text = data.candidates?.[0]?.content?.parts?.map((part) => part.text ?? "").join("").trim();
    if (!text) {
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
        failureDebug: {
          provider_response_text: trimDebugText(text),
        },
      };
    }
  } catch (error) {
    if (isAbortError(error)) {
      return {
        ok: false,
        retryable: true,
        errorCode: "TIMEOUT",
        reason: `Gemini request timed out after ${config.timeoutMs}ms`,
      };
    }
    const reason = toReason(error);
    return {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: `Gemini request error: ${reason}`,
    };
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

export async function callResearchProvider(
  candidate: AiCandidate,
  prompt: string,
  config: ResearchProviderConfig
): Promise<ResearchProviderResult> {
  if (!isAiApiCallAllowed()) {
    return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: AI_CALLS_BLOCKED_REASON };
  }

  if (candidate.provider === "openai") {
    if (!config.openAiApiKey) {
      return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "OPENAI_API_KEY is missing" };
    }
    return callOpenAi(prompt, candidate.model, config.openAiApiKey, config);
  }

  if (candidate.provider === "claude") {
    if (!config.anthropicApiKey) {
      return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "ANTHROPIC_API_KEY is missing" };
    }
    return callClaude(prompt, candidate.model, config.anthropicApiKey, config);
  }

  if (!config.geminiApiKey) {
    return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "GEMINI_API_KEY is missing" };
  }
  return callGemini(prompt, candidate.model, config.geminiApiKey, config);
}

export function hasProviderApiKey(provider: AiProvider, config: ResearchProviderConfig): boolean {
  if (provider === "openai") {
    return Boolean(config.openAiApiKey);
  }
  if (provider === "claude") {
    return Boolean(config.anthropicApiKey);
  }
  return Boolean(config.geminiApiKey);
}
