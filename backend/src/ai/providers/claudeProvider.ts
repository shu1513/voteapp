import type {
  EnrichStateResourcesConfig,
  EnrichStateResourcesInput,
  ProviderGenerateResult,
} from "../types.js";
import { AI_CALLS_BLOCKED_REASON, isAiApiCallAllowed } from "../aiCallGuard.js";
import { buildRetryFeedbackPromptLines } from "../retryFeedback.js";
import { buildStateResourcesPrompt } from "./stateResourcesPrompt.js";
import {
  extractProviderRateLimitDebugHeaders,
  updateProviderModelCooldownFromHeaders,
  waitForProviderModelCooldown,
} from "../providerRateLimitGate.js";
import { shouldSetExplicitClaudeTemperature } from "../modelRequestCapabilities.js";

const ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages";

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function trimDebugText(input: string, maxChars = 20_000): string {
  if (input.length <= maxChars) {
    return input;
  }
  return `${input.slice(0, maxChars)}...`;
}

/**
 * Extracts a JSON object string from plain text or fenced markdown output.
 */
function extractJsonCandidate(text: string): string {
  const trimmed = text.trim();

  const fenced = /```(?:json)?\s*([\s\S]*?)\s*```/i.exec(trimmed);
  if (fenced?.[1]) {
    return fenced[1].trim();
  }

  return trimmed;
}

export async function claudeProvider(
  input: EnrichStateResourcesInput,
  config: EnrichStateResourcesConfig
): Promise<ProviderGenerateResult> {
  if (!isAiApiCallAllowed()) {
    return {
      ok: false,
      retryable: false,
      errorCode: "CONFIGURATION_ERROR",
      reason: AI_CALLS_BLOCKED_REASON,
    };
  }

  if (!config.anthropicApiKey) {
    return {
      ok: false,
      retryable: false,
      errorCode: "CONFIGURATION_ERROR",
      reason: "ANTHROPIC_API_KEY is required when AI_PROVIDER=claude",
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.timeoutMs);
  const retryFeedbackLines = buildRetryFeedbackPromptLines(input.retryFeedback);
  const prompt = buildStateResourcesPrompt(input, retryFeedbackLines);
  const promptDebugMeta = {
    provider_prompt_variant: input.promptVariant ?? "default",
    provider_prompt_has_retry_feedback: retryFeedbackLines.length > 0,
    provider_prompt_retry_feedback_snapshot: retryFeedbackLines.length > 0 ? retryFeedbackLines.join("\n") : null,
  } as const;

  try {
    await waitForProviderModelCooldown("claude", config.model);

    const requestBody: Record<string, unknown> = {
      model: config.model,
      max_tokens: 2000,
      system: "You are a strict JSON generator for civic data. Use evidence-based factual summaries only.",
      messages: [
        {
          role: "user",
          content: prompt,
        },
      ],
    };
    if (shouldSetExplicitClaudeTemperature(config.model)) {
      requestBody.temperature = 0;
    }
    const headers: Record<string, string> = {
      "x-api-key": config.anthropicApiKey,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    };
    requestBody.tools = [
      {
        type: "web_search_20250305",
        name: "web_search",
        max_uses: Math.max(1, Math.floor(config.anthropicWebSearchMaxUses ?? 3)),
      },
    ];
    headers["anthropic-beta"] = "web-search-2025-03-05";

    const response = await fetch(ANTHROPIC_MESSAGES_URL, {
      method: "POST",
      headers,
      body: JSON.stringify(requestBody),
      signal: controller.signal,
    });

    if (!response.ok) {
      const rateLimitHeaders = extractProviderRateLimitDebugHeaders(response.headers);
      updateProviderModelCooldownFromHeaders("claude", config.model, response.headers, {
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
            ...promptDebugMeta,
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
            ...promptDebugMeta,
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
          ...promptDebugMeta,
          provider_response_text: trimDebugText(bodyText),
          provider_rate_limit_headers: rateLimitHeaders,
        },
      };
    }

    updateProviderModelCooldownFromHeaders("claude", config.model, response.headers);

    const data = (await response.json()) as {
      content?: Array<{ type?: string; text?: string }>;
    };

    const text = data.content?.find((part) => part.type === "text")?.text;
    if (!text || text.trim().length === 0) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "Claude returned empty content",
      };
    }

    try {
      const parsed = JSON.parse(extractJsonCandidate(text));
      return { ok: true, rawPayload: parsed, rawText: text, debugMeta: promptDebugMeta };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `Claude content was not valid JSON: ${toReason(error)}`,
        failureDebug: {
          ...promptDebugMeta,
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
        reason: `Claude request timed out after ${config.timeoutMs}ms`,
      };
    }

    return {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: `Claude request error: ${reason}`,
    };
  } finally {
    clearTimeout(timeout);
  }
}
