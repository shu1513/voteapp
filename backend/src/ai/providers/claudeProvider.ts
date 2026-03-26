import type {
  EnrichStateResourcesConfig,
  EnrichStateResourcesInput,
  ProviderGenerateResult,
} from "../types.js";

const ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages";

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
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

export async function claudeProvider(
  input: EnrichStateResourcesInput,
  config: EnrichStateResourcesConfig
): Promise<ProviderGenerateResult> {
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

  try {
    const response = await fetch(ANTHROPIC_MESSAGES_URL, {
      method: "POST",
      headers: {
        "x-api-key": config.anthropicApiKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: config.model,
        max_tokens: 2000,
        temperature: 0,
        system: "You are a strict JSON generator for civic data. Use evidence-based factual summaries only.",
        messages: [
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
          reason: `Claude rate limit: ${bodyText}`,
        };
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
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "Claude returned empty content",
      };
    }

    try {
      const parsed = JSON.parse(extractJsonCandidate(text));
      return { ok: true, rawPayload: parsed };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `Claude content was not valid JSON: ${toReason(error)}`,
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
