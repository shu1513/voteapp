import type {
  EnrichStateResourcesInput,
  EnrichStateResourcesConfig,
  ProviderGenerateResult,
} from "../types.js";
import { buildRetryFeedbackPromptLines } from "../retryFeedback.js";
import { buildScopedOpenAiJsonSchema } from "./stateResourceScopedOutput.js";
import { buildStateResourcesPrompt } from "./stateResourcesPrompt.js";

const OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions";
const SOURCES_BUCKET_SCHEMA = {
  type: "array",
  minItems: 1,
  items: { type: "string" },
} as const;

const STATE_RESOURCE_JSON_SCHEMA = {
  name: "state_resource_payload",
  strict: true,
  schema: {
    type: "object",
    additionalProperties: false,
    required: [
      "polling_place_url",
      "mail_voting_available",
      "mail_ballot_request_deadline_rule",
      "mail_ballot_return_deadline_rule",
      "mail_ballot_return_deadline_type",
      "early_voting_available",
      "early_voting_start_date_rule",
      "early_voting_end_date_rule",
      "polling_hours",
      "id_requirements",
      "same_day_registration_available",
      "online_registration_available",
      "online_registration_deadline_rule",
      "in_person_registration_deadline_rule",
      "sources",
    ],
    properties: {
      polling_place_url: { type: "string" },
      mail_voting_available: { type: "boolean" },
      mail_ballot_request_deadline_rule: {
        anyOf: [{ type: "string" }, { type: "null" }],
      },
      mail_ballot_return_deadline_rule: {
        anyOf: [{ type: "string" }, { type: "null" }],
      },
      mail_ballot_return_deadline_type: {
        anyOf: [{ type: "string", enum: ["postmarked_by", "received_by"] }, { type: "null" }],
      },
      early_voting_available: { type: "boolean" },
      early_voting_start_date_rule: {
        anyOf: [{ type: "string" }, { type: "null" }],
      },
      early_voting_end_date_rule: {
        anyOf: [{ type: "string" }, { type: "null" }],
      },
      polling_hours: { type: "string" },
      id_requirements: {
        type: "string",
        enum: [
          "Strict photo ID",
          "Strict non-photo ID",
          "Non-strict photo ID",
          "Non-strict, non-photo ID",
          "No document required to vote",
        ],
      },
      same_day_registration_available: { type: "boolean" },
      online_registration_available: { type: "boolean" },
      online_registration_deadline_rule: {
        anyOf: [{ type: "string" }, { type: "null" }],
      },
      in_person_registration_deadline_rule: { type: "string" },
      sources: {
        type: "object",
        additionalProperties: false,
        required: [
          "polling_place_url",
          "mail_voting_available",
          "mail_ballot_request_deadline_rule",
          "mail_ballot_return_deadline_rule",
          "mail_ballot_return_deadline_type",
          "early_voting_available",
          "early_voting_start_date_rule",
          "early_voting_end_date_rule",
          "polling_hours",
          "id_requirements",
          "same_day_registration_available",
          "online_registration_available",
          "online_registration_deadline_rule",
          "in_person_registration_deadline_rule",
        ],
        properties: {
          polling_place_url: SOURCES_BUCKET_SCHEMA,
          mail_voting_available: SOURCES_BUCKET_SCHEMA,
          mail_ballot_request_deadline_rule: SOURCES_BUCKET_SCHEMA,
          mail_ballot_return_deadline_rule: SOURCES_BUCKET_SCHEMA,
          mail_ballot_return_deadline_type: SOURCES_BUCKET_SCHEMA,
          early_voting_available: SOURCES_BUCKET_SCHEMA,
          early_voting_start_date_rule: SOURCES_BUCKET_SCHEMA,
          early_voting_end_date_rule: SOURCES_BUCKET_SCHEMA,
          polling_hours: SOURCES_BUCKET_SCHEMA,
          id_requirements: SOURCES_BUCKET_SCHEMA,
          same_day_registration_available: SOURCES_BUCKET_SCHEMA,
          online_registration_available: SOURCES_BUCKET_SCHEMA,
          online_registration_deadline_rule: SOURCES_BUCKET_SCHEMA,
          in_person_registration_deadline_rule: SOURCES_BUCKET_SCHEMA,
        },
      },
    },
  },
} as const;

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
  // GPT-5-family chat completions require default temperature behavior.
  return !model.toLowerCase().startsWith("gpt-5");
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
  const scopedSchema = buildScopedOpenAiJsonSchema(input.fieldGroup);
  const promptDebugMeta = {
    provider_prompt_variant: input.promptVariant ?? "default",
    provider_prompt_has_retry_feedback: retryFeedbackLines.length > 0,
    provider_prompt_retry_feedback_snapshot: retryFeedbackLines.length > 0 ? retryFeedbackLines.join("\n") : null,
  } as const;

  try {
    const requestBody: Record<string, unknown> = {
      model: config.model,
      response_format: {
        type: "json_schema",
        json_schema: scopedSchema ?? STATE_RESOURCE_JSON_SCHEMA,
      },
      messages: [
        {
          role: "system",
          content:
            "You are a strict JSON generator for civic data. Use evidence-based factual summaries only.",
        },
        {
          role: "user",
          content: prompt,
        },
      ],
    };

    if (shouldSetExplicitTemperature(config.model)) {
      requestBody.temperature = 0;
    }

    const response = await fetch(OPENAI_CHAT_COMPLETIONS_URL, {
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
          reason: `OpenAI rate limit: ${bodyText}`,
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
          reason: `OpenAI temporary error ${response.status}: ${bodyText}`,
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
        reason: `OpenAI request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          ...promptDebugMeta,
          provider_response_text: trimDebugText(bodyText),
        },
      };
    }

    const data = (await response.json()) as {
      choices?: Array<{ message?: { content?: string } }>;
    };

    const content = data.choices?.[0]?.message?.content;
    if (!content || content.trim().length === 0) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "OpenAI returned empty content",
      };
    }

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
        },
      };
    }

    try {
      const parsed = JSON.parse(extraction.candidate);
      return { ok: true, rawPayload: parsed, rawText: content, debugMeta: promptDebugMeta };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `OpenAI content was not valid JSON: ${toReason(error)}`,
        failureDebug: {
          ...promptDebugMeta,
          provider_response_text: trimDebugText(content),
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
        reason: `OpenAI request timed out after ${config.timeoutMs}ms`,
      };
    }

    return {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: `OpenAI request error: ${reason}`,
    };
  } finally {
    clearTimeout(timeout);
  }
}
