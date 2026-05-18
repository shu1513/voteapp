import type {
  EnrichStateResourcesInput,
  EnrichStateResourcesConfig,
  ProviderGenerateResult,
} from "../types.js";
import { buildRetryFeedbackPromptLines } from "../retryFeedback.js";
import { buildStateResourcesPrompt } from "./stateResourcesPrompt.js";

const OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses";

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

function extractLeadingJsonObject(
  text: string
): { candidate: string; trailing: string } | null {
  const trimmed = text.trimStart();
  if (!trimmed.startsWith("{")) {
    return null;
  }

  let depth = 0;
  let inString = false;
  let escaping = false;

  for (let i = 0; i < trimmed.length; i += 1) {
    const ch = trimmed[i];

    if (inString) {
      if (escaping) {
        escaping = false;
      } else if (ch === "\\") {
        escaping = true;
      } else if (ch === "\"") {
        inString = false;
      }
      continue;
    }

    if (ch === "\"") {
      inString = true;
      continue;
    }

    if (ch === "{") {
      depth += 1;
      continue;
    }

    if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        return {
          candidate: trimmed.slice(0, i + 1),
          trailing: trimmed.slice(i + 1),
        };
      }
      if (depth < 0) {
        return null;
      }
    }
  }

  return null;
}

function shouldSetExplicitTemperature(model: string): boolean {
  // GPT-5-family should use default temperature behavior.
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

export async function openAiProvider(
  input: EnrichStateResourcesInput,
  config: EnrichStateResourcesConfig
): Promise<ProviderGenerateResult> {
  if (!config.openAiApiKey) {
    return {
      ok: false,
      retryable: false,
      errorCode: "CONFIGURATION_ERROR",
      reason: "OPENAI_API_KEY is required when AI_PROVIDER=openai",
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
    const requestBody: Record<string, unknown> = {
      model: config.model,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text: "You are a strict JSON generator for civic data. Use evidence-based factual summaries only.",
            },
          ],
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

    if (shouldSetExplicitTemperature(config.model)) {
      requestBody.temperature = 0;
    }

    const response = await fetch(OPENAI_RESPONSES_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.openAiApiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal,
    });

    if (!response.ok) {
      const bodyText = await response.text();
      if (response.status === 429) {
        return {
          ok: false,
          retryable: true,
          errorCode: "RATE_LIMIT",
          reason: `OpenAI responses rate limit: ${bodyText}`,
          failureDebug: {
            ...promptDebugMeta,
            provider_response_text: trimDebugText(bodyText),
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
            ...promptDebugMeta,
            provider_response_text: trimDebugText(bodyText),
          },
        };
      }

      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `OpenAI responses request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          ...promptDebugMeta,
          provider_response_text: trimDebugText(bodyText),
        },
      };
    }

    const data = (await response.json()) as Record<string, unknown>;
    const content = extractResponsesOutputText(data);
    if (!content || content.trim().length === 0) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "OpenAI responses returned empty assistant text",
        failureDebug: {
          ...promptDebugMeta,
          provider_response_payload: data,
        },
      };
    }

    const webSearchSources = extractOpenAiWebSearchSources(data);

    const extraction = extractLeadingJsonObject(content);
    if (!extraction) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "OpenAI content did not start with a valid JSON object",
        failureDebug: {
          ...promptDebugMeta,
          provider_response_text: trimDebugText(content),
          web_search_sources: webSearchSources,
          web_search_sources_count: webSearchSources.length,
        },
      };
    }

    const trailing = extraction.trailing.trim();
    if (trailing.length > 0 && trailing !== extraction.candidate.trim()) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "OpenAI content had non-JSON or non-duplicate trailing output after first JSON object",
        failureDebug: {
          ...promptDebugMeta,
          provider_response_text: trimDebugText(content),
          web_search_sources: webSearchSources,
          web_search_sources_count: webSearchSources.length,
        },
      };
    }

    try {
      const parsed = JSON.parse(extraction.candidate);
      return {
        ok: true,
        rawPayload: parsed,
        rawText: content,
        debugMeta: {
          ...promptDebugMeta,
          openai_api_mode: "responses_web_search",
          web_search_sources: webSearchSources,
          web_search_sources_count: webSearchSources.length,
        },
      };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `OpenAI content was not valid JSON: ${toReason(error)}`,
        failureDebug: {
          ...promptDebugMeta,
          provider_response_text: trimDebugText(content),
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
        reason: `OpenAI responses request timed out after ${config.timeoutMs}ms`,
      };
    }

    return {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: `OpenAI responses request error: ${reason}`,
    };
  } finally {
    clearTimeout(timeout);
  }
}
