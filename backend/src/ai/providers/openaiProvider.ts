import type {
  EnrichStateResourcesInput,
  EnrichStateResourcesConfig,
  ProviderGenerateResult,
} from "../types.js";

const OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions";
const SOURCE_CITATION_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["source_name", "source_url"],
  properties: {
    source_name: { type: "string" },
    source_url: { type: "string" },
  },
} as const;

const SOURCES_BUCKET_SCHEMA = {
  type: "array",
  minItems: 1,
  items: SOURCE_CITATION_SCHEMA,
} as const;

const STATE_RESOURCE_JSON_SCHEMA = {
  name: "state_resource_payload",
  strict: true,
  schema: {
    type: "object",
    additionalProperties: false,
    required: [
      "state_fips",
      "state_abbreviation",
      "state_name",
      "polling_place_url",
      "voter_registration_url",
      "vote_by_mail_info",
      "polling_hours",
      "id_requirements",
      "sources",
    ],
    properties: {
      state_fips: { type: "string" },
      state_abbreviation: { type: "string" },
      state_name: { type: "string" },
      polling_place_url: { type: "string" },
      voter_registration_url: { type: "string" },
      vote_by_mail_info: { type: "string" },
      polling_hours: { type: "string" },
      id_requirements: { type: "string" },
      sources: {
        type: "object",
        additionalProperties: false,
        required: [
          "polling_place_url",
          "voter_registration_url",
          "vote_by_mail_info",
          "polling_hours",
          "id_requirements",
        ],
        properties: {
          polling_place_url: SOURCES_BUCKET_SCHEMA,
          voter_registration_url: SOURCES_BUCKET_SCHEMA,
          vote_by_mail_info: SOURCES_BUCKET_SCHEMA,
          polling_hours: SOURCES_BUCKET_SCHEMA,
          id_requirements: SOURCES_BUCKET_SCHEMA,
        },
      },
    },
  },
} as const;

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function buildPrompt(input: EnrichStateResourcesInput): string {
  return [
    "Return only one JSON object with these keys exactly:",
    "state_fips, state_abbreviation, state_name, polling_place_url, voter_registration_url, vote_by_mail_info, polling_hours, id_requirements, sources.",
    "sources must include keys: polling_place_url, voter_registration_url, vote_by_mail_info, polling_hours, id_requirements.",
    "Each sources[key] must be an array of {source_name, source_url}.",
    "Each source_url must exactly match one of the Evidence snippets URLs. Do not invent or rewrite URLs.",
    "polling_place_url and voter_registration_url must be URLs.",
    "vote_by_mail_info, polling_hours, and id_requirements must be plain-language text summaries, not URLs.",
    "For vote_by_mail_info: include at least one concrete state rule detail (e.g., request deadline, return deadline, postmark/received rule, or return methods).",
    "For polling_hours: include statewide opening/closing times when available; otherwise explicitly state that hours vary by county/precinct.",
    "For id_requirements: explicitly state whether voter ID is required at polls, and major exceptions if applicable.",
    "For sources.vote_by_mail_info, sources.polling_hours, and sources.id_requirements: include at least one citation each.",
    "Prefer official state/local election office or .gov sources for those three fields when available (not strictly required if unavailable in evidence).",
    "Do not output generic templates; each of those three fields must be specific to the draft state.",
    "Prefer official state/local election office polling-place URLs over aggregator URLs when evidence includes both.",
    "Do not add markdown fences or commentary.",
    "",
    "Draft input:",
    JSON.stringify(input.draft),
    "",
    "Evidence snippets:",
    JSON.stringify(input.evidence),
  ].join("\n");
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

  try {
    const response = await fetch(OPENAI_CHAT_COMPLETIONS_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.openAiApiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: config.model,
        temperature: 0,
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
            content: buildPrompt(input),
          },
        ],
      }),
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
        };
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
      return { ok: true, rawPayload: parsed };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `OpenAI content was not valid JSON: ${toReason(error)}`,
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
