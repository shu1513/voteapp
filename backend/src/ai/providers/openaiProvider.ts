import type {
  EnrichStateResourcesInput,
  EnrichStateResourcesConfig,
  PromptVariant,
  ProviderGenerateResult,
} from "../types.js";
import { buildRetryFeedbackPromptLines } from "../retryFeedback.js";

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
      "polling_hours",
      "id_requirements",
      "same_day_registration_available",
      "online_registration_available",
      "online_registration_deadline_rule",
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
      polling_hours: { type: "string" },
      id_requirements: { type: "string" },
      same_day_registration_available: { type: "boolean" },
      online_registration_available: { type: "boolean" },
      online_registration_deadline_rule: {
        anyOf: [{ type: "string" }, { type: "null" }],
      },
      sources: {
        type: "object",
        additionalProperties: false,
        required: [
          "polling_place_url",
          "mail_voting_available",
          "mail_ballot_request_deadline_rule",
          "mail_ballot_return_deadline_rule",
          "mail_ballot_return_deadline_type",
          "polling_hours",
          "id_requirements",
          "same_day_registration_available",
          "online_registration_available",
          "online_registration_deadline_rule",
        ],
        properties: {
          polling_place_url: SOURCES_BUCKET_SCHEMA,
          mail_voting_available: SOURCES_BUCKET_SCHEMA,
          mail_ballot_request_deadline_rule: SOURCES_BUCKET_SCHEMA,
          mail_ballot_return_deadline_rule: SOURCES_BUCKET_SCHEMA,
          mail_ballot_return_deadline_type: SOURCES_BUCKET_SCHEMA,
          polling_hours: SOURCES_BUCKET_SCHEMA,
          id_requirements: SOURCES_BUCKET_SCHEMA,
          same_day_registration_available: SOURCES_BUCKET_SCHEMA,
          online_registration_available: SOURCES_BUCKET_SCHEMA,
          online_registration_deadline_rule: SOURCES_BUCKET_SCHEMA,
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

function buildPromptVariantLines(promptVariant: PromptVariant | undefined): string[] {
  if (promptVariant !== "citation_repair") {
    return [];
  }

  return [
    "Citation-repair mode:",
    "- Keep the same factual meaning as prior attempt; replace broken citations only.",
    "- Replace blocked/not-found citations with different verifiable URLs.",
    "- Do not reuse any URL listed in failed_citation_urls.",
  ];
}

function buildPrompt(input: EnrichStateResourcesInput, retryFeedbackLines: string[]): string {
  const promptVariantLines = buildPromptVariantLines(input.promptVariant);

  return [
    "Return only one JSON object with these keys exactly:",
    "polling_place_url, mail_voting_available, mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, mail_ballot_return_deadline_type, polling_hours, id_requirements, same_day_registration_available, online_registration_available, online_registration_deadline_rule, sources.",
    "sources must include keys: polling_place_url, mail_voting_available, mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, mail_ballot_return_deadline_type, polling_hours, id_requirements, same_day_registration_available, online_registration_available, online_registration_deadline_rule.",
    "Each sources[key] must be an array of URL strings.",
    "Prefer using Evidence snippets URLs when possible.",
    "You may cite additional public URLs if they directly support the claim; do not invent or rewrite URLs.",
    "Per-field citation rule:",
    "- For each field, research until you find URL(s) that directly support the final statement.",
    "- Write the field only from those supporting URL(s).",
    "- In sources[field_name], include only URL(s) that were actually used to support that field's final text.",
    "- Do not include attempted URLs that lacked the needed information.",
    "polling_place_url must be a URL.",
    "For polling_place_url, start from polling reference seed URLs in Evidence snippets, then expand if needed.",
    "same_day_registration_available must be boolean true or false.",
    "online_registration_available must be boolean true or false.",
    "If online_registration_available is false, set online_registration_deadline_rule to null.",
    "online_registration_deadline_rule must be a short plain-language sentence (not URL) when online registration is available; otherwise null.",
    "For online_registration_available and online_registration_deadline_rule, start with the Vote.gov state registration reference URL in Evidence snippets (https://vote.gov/register/<state-name-lowercase>).",
    "You may use additional sources beyond that reference URL when needed; it is a starting point, not a restriction.",
    "mail_voting_available must be boolean true or false.",
    "If mail_voting_available is false, set mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, and mail_ballot_return_deadline_type to null.",
    "If mail_voting_available is true, set mail_ballot_return_deadline_rule and mail_ballot_return_deadline_type (postmarked_by or received_by).",
    "mail_ballot_request_deadline_rule and mail_ballot_return_deadline_rule must be short plain-language sentences (not URLs) when present.",
    "For mail-voting fields, start with the Vote.gov state registration reference URL in Evidence snippets (https://vote.gov/register/<state-name-lowercase>) before expanding to additional sources.",
    "For mail_ballot_return_deadline_rule: include a concrete state rule detail.",
    "polling_hours and id_requirements must be plain-language text summaries, not URLs.",
    "For polling_hours: include statewide opening/closing times when available; otherwise explicitly state that hours vary by county/precinct.",
    "For id_requirements: first sentence must be exactly one of these patterns with the draft state name:",
    "\"Voter ID is required at the polls in <STATE>.\" or \"Voter ID is not required at the polls in <STATE>.\"",
    "Then add one short sentence for major exceptions, if any.",
    "Do not use ambiguous first-sentence phrasing like \"may\", \"can depend\", or \"varies\" without explicitly saying required vs not required.",
    "For full-sentence summary fields (mail_ballot_request_deadline_rule when present, mail_ballot_return_deadline_rule when present, polling_hours, id_requirements), provide at least one citation each.",
    "For mail_voting_available, mail_ballot_return_deadline_type when present, same_day_registration_available, online_registration_available, and online_registration_deadline_rule, provide at least one citation each.",
    "sources.id_requirements must include at least one citation that directly supports the required/not-required claim in id_requirements.",
    "Self-check before final output: id_requirements must contain either \"is required\" or \"is not required\".",
    "Source guidance:",
    "- Prefer official election sources (.gov, secretary of state, county elections) when available and keep citations.",
    "- If official sources are hard to find, use reliable secondary sources and keep citations.",
    "- If sources disagree, do additional research and choose one final rule using this priority:",
    "  1) official state/county election source",
    "  2) most credible sources",
    "  3) most recent update/publication date",
    "- Keep summaries plain and practical.",
    "- URL quality rule: Do not cite URLs that are broken, login-only, or unrelated landing pages.",
    "Prefer official state/local election office polling-place URLs over aggregator URLs when evidence includes both.",
    "Do not add markdown fences or commentary.",
    ...(promptVariantLines.length > 0 ? ["", ...promptVariantLines] : []),
    ...(retryFeedbackLines.length > 0 ? ["", ...retryFeedbackLines] : []),
    "",
    "Draft input:",
    JSON.stringify(input.draft),
    "",
    "Evidence snippets:",
    JSON.stringify(input.evidence),
  ].join("\n");
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
  const prompt = buildPrompt(input, retryFeedbackLines);
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
        json_schema: STATE_RESOURCE_JSON_SCHEMA,
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

    try {
      const parsed = JSON.parse(content);
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
