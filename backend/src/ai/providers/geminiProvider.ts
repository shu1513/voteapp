import type {
  EnrichStateResourcesConfig,
  EnrichStateResourcesInput,
  ProviderGenerateResult,
} from "../types.js";

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
    "Do not add markdown fences or commentary.",
    "",
    "Draft input:",
    JSON.stringify(input.draft),
    "",
    "Evidence snippets:",
    JSON.stringify(input.evidence),
  ].join("\n");
}

export async function geminiProvider(
  input: EnrichStateResourcesInput,
  config: EnrichStateResourcesConfig
): Promise<ProviderGenerateResult> {
  if (!config.geminiApiKey) {
    return {
      ok: false,
      retryable: false,
      errorCode: "CONFIGURATION_ERROR",
      reason: "GEMINI_API_KEY is required when AI_PROVIDER=gemini",
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.timeoutMs);
  const url = `https://generativelanguage.googleapis.com/v1/models/${encodeURIComponent(
    config.model
  )}:generateContent?key=${encodeURIComponent(config.geminiApiKey)}`;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        generationConfig: {
          temperature: 0,
          responseMimeType: "application/json",
        },
        contents: [
          {
            role: "user",
            parts: [{ text: buildPrompt(input) }],
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
          reason: `Gemini rate limit: ${bodyText}`,
        };
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
      candidates?: Array<{
        content?: {
          parts?: Array<{ text?: string }>;
        };
      }>;
    };

    const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!text || text.trim().length === 0) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "Gemini returned empty content",
      };
    }

    try {
      const parsed = JSON.parse(text);
      return { ok: true, rawPayload: parsed };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `Gemini content was not valid JSON: ${toReason(error)}`,
      };
    }
  } catch (error) {
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return {
        ok: false,
        retryable: true,
        errorCode: "TIMEOUT",
        reason: `Gemini request timed out after ${config.timeoutMs}ms`,
      };
    }

    return {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: `Gemini request error: ${reason}`,
    };
  } finally {
    clearTimeout(timeout);
  }
}
