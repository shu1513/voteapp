import type {
  EnrichStateResourcesConfig,
  EnrichStateResourcesInput,
  PromptVariant,
  ProviderGenerateResult,
} from "../types.js";
import { buildRetryFeedbackPromptLines } from "../retryFeedback.js";

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
    "state_fips, state_abbreviation, state_name, polling_place_url, voter_registration_url, vote_by_mail_info, polling_hours, id_requirements, sources.",
    "sources must include keys: polling_place_url, voter_registration_url, vote_by_mail_info, polling_hours, id_requirements.",
    "Each sources[key] must be an array of {source_name, source_url}.",
    "Prefer using Evidence snippets URLs when possible.",
    "You may cite additional public URLs if they directly support the claim; do not invent or rewrite URLs.",
    "polling_place_url and voter_registration_url must be URLs.",
    "vote_by_mail_info, polling_hours, and id_requirements must be plain-language text summaries, not URLs.",
    "For vote_by_mail_info: include at least one concrete state rule detail (e.g., request deadline, return deadline, postmark/received rule, or return methods).",
    "For polling_hours: include statewide opening/closing times when available; otherwise explicitly state that hours vary by county/precinct.",
    "For id_requirements: first sentence must be exactly one of these patterns with the draft state name:",
    "\"Voter ID is required at the polls in <STATE>.\" or \"Voter ID is not required at the polls in <STATE>.\"",
    "Then add one short sentence for major exceptions, if any.",
    "Do not use ambiguous first-sentence phrasing like \"may\", \"can depend\", or \"varies\" without explicitly saying required vs not required.",
    "For full-sentence summary fields (vote_by_mail_info, polling_hours, id_requirements), provide at least one citation each.",
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
    "Do not output generic templates; each of those three fields must be specific to the draft state.",
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
  const retryFeedbackLines = buildRetryFeedbackPromptLines(input.retryFeedback);
  const prompt = buildPrompt(input, retryFeedbackLines);
  const promptDebugMeta = {
    provider_prompt_variant: input.promptVariant ?? "default",
    provider_prompt_has_retry_feedback: retryFeedbackLines.length > 0,
    provider_prompt_retry_feedback_snapshot: retryFeedbackLines.length > 0 ? retryFeedbackLines.join("\n") : null,
  } as const;
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
        },
        contents: [
          {
            role: "user",
            parts: [{ text: prompt }],
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
          reason: `Gemini temporary error ${response.status}: ${bodyText}`,
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
        reason: `Gemini request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          ...promptDebugMeta,
          provider_response_text: trimDebugText(bodyText),
        },
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
      const parsed = JSON.parse(extractJsonCandidate(text));
      return { ok: true, rawPayload: parsed, rawText: text, debugMeta: promptDebugMeta };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `Gemini content was not valid JSON: ${toReason(error)}`,
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
