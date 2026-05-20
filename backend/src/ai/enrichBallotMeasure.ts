import { BALLOT_MEASURES_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  extractProviderRateLimitDebugHeaders,
  updateProviderModelCooldownFromHeaders,
  waitForProviderModelCooldown,
} from "./providerRateLimitGate.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";
import type { AiProvider } from "./types.js";
import { buildBallotMeasuresPrompt } from "./providers/ballotMeasuresPrompt.js";
import { verifyHttpUrlReachability } from "./urlReachability.js";

const OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses";
const ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages";
const GEMINI_GENERATE_CONTENT_URL = "https://generativelanguage.googleapis.com/v1beta/models";

type RetryableErrorCode = "RATE_LIMIT" | "TIMEOUT" | "TEMP_PROVIDER_ERROR";
type PermanentErrorCode = "INVALID_JSON" | "SCHEMA_MISMATCH" | "CONFIGURATION_ERROR";

type EnrichmentFailure = {
  ok: false;
  retryable: boolean;
  errorCode: RetryableErrorCode | PermanentErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: string;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

type BallotMeasureValidationResult =
  | {
      ok: true;
      officialMeasureUrl: string;
      whatYesMeans: string;
      whatNoMeans: string;
      sources: string[];
    }
  | {
      ok: false;
      reason: string;
      blockedUrls: string[];
      failureDebug?: Record<string, unknown>;
    };

export type BallotMeasureAiInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  seedUrls: readonly string[];
};

export type BallotMeasureAiConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type BallotMeasureAiResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      officialMeasureUrl: string;
      whatYesMeans: string;
      whatNoMeans: string;
      researchUrls: string[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | EnrichmentFailure;

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

function extractOpenAiWebSearchUrls(responsePayload: unknown): string[] {
  if (typeof responsePayload !== "object" || responsePayload === null) {
    return [];
  }
  const input = responsePayload as Record<string, unknown>;
  const output = input.output;
  if (!Array.isArray(output)) {
    return [];
  }

  const urls: string[] = [];
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
      if (typeof sourceRecord.url !== "string") {
        continue;
      }
      const normalized = normalizeHttpUrl(sourceRecord.url);
      if (!normalized || seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      urls.push(normalized);
    }
  }

  return urls;
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

function parseBallotMeasureAiPayload(payload: unknown): {
  ok: true;
  officialMeasureUrl: string;
  whatYesMeans: string;
  whatNoMeans: string;
  sources: string[];
} | {
  ok: false;
  reason: string;
} {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (typeof input.official_measure_url !== "string") {
    return { ok: false, reason: "official_measure_url must be string" };
  }
  if (typeof input.what_yes_means !== "string") {
    return { ok: false, reason: "what_yes_means must be string" };
  }
  if (typeof input.what_no_means !== "string") {
    return { ok: false, reason: "what_no_means must be string" };
  }
  if (!Array.isArray(input.sources)) {
    return { ok: false, reason: "sources must be array" };
  }

  const officialMeasureUrl = normalizeHttpUrl(input.official_measure_url);
  if (!officialMeasureUrl) {
    return { ok: false, reason: "official_measure_url must be valid http(s) URL" };
  }

  const whatYesMeans = input.what_yes_means.trim();
  const whatNoMeans = input.what_no_means.trim();
  if (whatYesMeans.length === 0 || whatNoMeans.length === 0) {
    return { ok: false, reason: "what_yes_means/what_no_means must be non-empty" };
  }

  const sources: string[] = [];
  const seenSourceUrls = new Set<string>();
  for (const raw of input.sources) {
    if (typeof raw !== "string") {
      return { ok: false, reason: "sources must contain URL strings" };
    }
    const normalized = normalizeHttpUrl(raw);
    if (!normalized) {
      return { ok: false, reason: "sources must contain valid http(s) URLs" };
    }
    if (seenSourceUrls.has(normalized)) {
      continue;
    }
    seenSourceUrls.add(normalized);
    sources.push(normalized);
  }
  if (sources.length === 0) {
    return { ok: false, reason: "sources must contain at least one URL" };
  }

  return {
    ok: true,
    officialMeasureUrl,
    whatYesMeans,
    whatNoMeans,
    sources,
  };
}

async function validateBallotMeasurePayload(
  payload: unknown,
  timeoutMs: number
): Promise<BallotMeasureValidationResult> {
  const parsed = parseBallotMeasureAiPayload(payload);
  if (!parsed.ok) {
    return { ok: false, reason: parsed.reason, blockedUrls: [] };
  }

  const verificationTimeoutMs = Math.min(timeoutMs, 15_000);
  const officialVerification = await verifyHttpUrlReachability(parsed.officialMeasureUrl, {
    timeoutMs: verificationTimeoutMs,
    allowStatusCodes: [403],
  });
  if (!officialVerification.ok) {
    return {
      ok: false,
      reason: `official_measure_url is not reachable: ${officialVerification.reason}`,
      blockedUrls: [parsed.officialMeasureUrl],
      failureDebug: {
        official_measure_url: parsed.officialMeasureUrl,
        official_measure_url_verification_reason: officialVerification.reason,
      },
    };
  }

  const uniqueSourceUrls = [...new Set(parsed.sources)];
  const sourceChecks = await Promise.all(
    uniqueSourceUrls.map(async (url) => ({
      url,
      verification: await verifyHttpUrlReachability(url, {
        timeoutMs: Math.min(timeoutMs, 8_000),
        allowStatusCodes: [403],
      }),
    }))
  );

  const badSourceChecks = sourceChecks.flatMap((check) =>
    check.verification.ok
      ? []
      : [
          {
            url: check.url,
            reason: check.verification.reason,
          },
        ]
  );
  if (badSourceChecks.length > 0) {
    const firstBad = badSourceChecks[0];
    const badUrls = badSourceChecks.map((check) => check.url);
    return {
      ok: false,
      reason: `source URL is not reachable: ${firstBad.url} (${firstBad.reason})`,
      blockedUrls: badUrls,
      failureDebug: {
        bad_source_urls: badSourceChecks.map((check) => ({
          url: check.url,
          reason: check.reason,
        })),
      },
    };
  }

  const normalizedSources = sourceChecks
    .map((check) => (check.verification.ok ? check.verification.finalUrl : null))
    .filter((url): url is string => typeof url === "string");

  return {
    ok: true,
    officialMeasureUrl: officialVerification.finalUrl,
    whatYesMeans: parsed.whatYesMeans,
    whatNoMeans: parsed.whatNoMeans,
    sources: [...new Set(normalizedSources)],
  };
}

async function callOpenAi(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | EnrichmentFailure> {
  let timeout: ReturnType<typeof setTimeout> | null = null;
  try {
    await waitForProviderModelCooldown("openai", model);
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), timeoutMs);

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
      const webSearchUrls = extractOpenAiWebSearchUrls(data);
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
        reason: `OpenAI returned invalid JSON: ${toReason(error)}`,
        failureDebug: {
          provider_response_text: trimDebugText(text),
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
        reason: `OpenAI request timed out after ${timeoutMs}ms`,
      };
    }
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
  timeoutMs: number,
  webSearchMaxUses = 3
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | EnrichmentFailure> {
  let timeout: ReturnType<typeof setTimeout> | null = null;

  try {
    await waitForProviderModelCooldown("claude", model);
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), timeoutMs);

    const requestBody: Record<string, unknown> = {
      model,
      max_tokens: 4000,
      temperature: 0,
      system: "Return strict JSON only.",
      messages: [{ role: "user", content: prompt }],
      tools: [
        {
          type: "web_search_20250305",
          name: "web_search",
          max_uses: Math.max(1, Math.floor(webSearchMaxUses)),
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
        retryAfterBufferMs: 10_000,
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
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return {
        ok: false,
        retryable: true,
        errorCode: "TIMEOUT",
        reason: `Claude request timed out after ${timeoutMs}ms`,
      };
    }

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
}

async function callGemini(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | EnrichmentFailure> {
  let timeout: ReturnType<typeof setTimeout> | null = null;

  try {
    await waitForProviderModelCooldown("gemini", model);
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), timeoutMs);

    const response = await fetch(
      `${GEMINI_GENERATE_CONTENT_URL}/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: 0,
            responseMimeType: "application/json",
          },
        }),
        signal: controller.signal,
      }
    );

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
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return {
        ok: false,
        retryable: true,
        errorCode: "TIMEOUT",
        reason: `Gemini request timed out after ${timeoutMs}ms`,
      };
    }
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

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: BallotMeasureAiConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | EnrichmentFailure> {
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
    return callClaude(prompt, candidate.model, config.anthropicApiKey, config.timeoutMs, config.anthropicWebSearchMaxUses);
  }

  if (!config.geminiApiKey) {
    return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "GEMINI_API_KEY is missing" };
  }
  return callGemini(prompt, candidate.model, config.geminiApiKey, config.timeoutMs);
}

export function buildBallotMeasureAiConfigFromEnv(): BallotMeasureAiConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichBallotMeasure(
  input: BallotMeasureAiInput,
  config: BallotMeasureAiConfig,
  candidates: readonly AiCandidate[] = BALLOT_MEASURES_AI_CANDIDATES
): Promise<BallotMeasureAiResult> {
  const failures: ProviderFailureAttempt[] = [];
  const cumulativeBlockedUrlFeedback = new Set<string>();

  for (const candidate of candidates) {
    let reviewFeedbackLines: string[] = [...cumulativeBlockedUrlFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildBallotMeasuresPrompt({
        ...input,
        reviewFeedbackLines,
      });
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
        break;
      }

      const validation = await validateBallotMeasurePayload(generated.parsed, config.timeoutMs);
      if (!validation.ok) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: validation.reason,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            provider_response_text: trimDebugText(generated.rawText),
            ...(validation.failureDebug ?? {}),
          },
        });
        const canRetrySameModel = attempt === 0;
        if (canRetrySameModel) {
          for (const blockedUrl of validation.blockedUrls) {
            cumulativeBlockedUrlFeedback.add(`Do not use or cite this URL: ${blockedUrl}`);
          }
          cumulativeBlockedUrlFeedback.add(`Fix this validation issue: ${validation.reason}`);
          reviewFeedbackLines = [...cumulativeBlockedUrlFeedback].slice(0, 20);
          continue;
        }
        break;
      }

      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        officialMeasureUrl: validation.officialMeasureUrl,
        whatYesMeans: validation.whatYesMeans,
        whatNoMeans: validation.whatNoMeans,
        researchUrls: (() => {
          const urls = new Set<string>();
          for (const url of validation.sources) {
            urls.add(url);
          }
          if (generated.debugMeta && Array.isArray(generated.debugMeta.web_search_urls)) {
            for (const raw of generated.debugMeta.web_search_urls) {
              if (typeof raw !== "string") {
                continue;
              }
              const normalized = normalizeHttpUrl(raw);
              if (normalized) {
                urls.add(normalized);
              }
            }
          }
          urls.add(validation.officialMeasureUrl);
          return [...urls];
        })(),
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
          ...(generated.debugMeta ?? {}),
        },
      };
    }
  }

  const finalFailure = failures[failures.length - 1];
  const anyRetryable = failures.some((failure) => failure.retryable);
  const firstRetryable = failures.find((failure) => failure.retryable);
  const firstPermanent = failures.find((failure) => !failure.retryable);
  const selected = anyRetryable ? (firstRetryable ?? finalFailure) : (firstPermanent ?? finalFailure);

  return {
    ok: false,
    retryable: selected?.retryable ?? false,
    errorCode: (selected?.errorCode as RetryableErrorCode | PermanentErrorCode | undefined) ?? "TEMP_PROVIDER_ERROR",
    reason: selected?.reason ?? "No AI candidates available for ballot-measure enrichment",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(
        buildBallotMeasuresPrompt({
          ...input,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
}
