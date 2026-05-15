import type {
  EnrichStateResourcesConfig,
  EnrichStateResourcesInput,
  PromptVariant,
  ProviderGenerateResult,
} from "../types.js";
import { buildRetryFeedbackPromptLines } from "../retryFeedback.js";
import { buildScopedPrompt } from "./stateResourceScopedOutput.js";

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
  const scopedPrompt = buildScopedPrompt(input);
  if (scopedPrompt) {
    return [
      scopedPrompt,
      "Prefer official election sources (.gov, secretary of state, county elections) when available.",
      "Do not add markdown fences or commentary.",
      ...(promptVariantLines.length > 0 ? ["", ...promptVariantLines] : []),
      ...(retryFeedbackLines.length > 0 ? ["", ...retryFeedbackLines] : []),
      "",
      "Draft input:",
      JSON.stringify(input.draft),
      "",
      "Evidence URLs:",
      JSON.stringify(input.evidence),
    ].join("\n");
  }

  return [
    "Return only one JSON object with these keys exactly:",
    "polling_place_url, mail_voting_available, mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, mail_ballot_return_deadline_type, early_voting_available, early_voting_start_date_rule, early_voting_end_date_rule, polling_hours, id_requirements, same_day_registration_available, online_registration_available, online_registration_deadline_rule, in_person_registration_deadline_rule, sources.",
    "sources must include keys: polling_place_url, mail_voting_available, mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, mail_ballot_return_deadline_type, early_voting_available, early_voting_start_date_rule, early_voting_end_date_rule, polling_hours, id_requirements, same_day_registration_available, online_registration_available, online_registration_deadline_rule, in_person_registration_deadline_rule.",
    "Each sources[key] must be an array of URL strings.",
    "Prefer using Evidence URLs when possible.",
    "You may cite additional public URLs if they directly support the claim; do not invent or rewrite URLs.",
    "Per-field citation rule:",
    "- For each field, research until you find URL(s) that directly support the final statement.",
    "- Write the field only from those supporting URL(s).",
    "- In sources[field_name], include only URL(s) that were actually used to support that field's final text.",
    "- Do not include attempted URLs that lacked the needed information.",
    "polling_place_url must be a URL.",
    "For polling_place_url, start from polling reference seed URLs in Evidence URLs, then expand if needed.",
    "same_day_registration_available must be boolean true or false.",
    "For same_day_registration_available, start with the NCSL same-day registration reference URL in Evidence URLs (https://www.ncsl.org/elections-and-campaigns/same-day-voter-registration).",
    "mail_voting_available must be boolean true or false.",
    "If mail_voting_available is false, set mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, and mail_ballot_return_deadline_type to null.",
    "If mail_voting_available is true, set mail_ballot_return_deadline_rule and mail_ballot_return_deadline_type (postmarked_by or received_by).",
    "early_voting_available must be boolean true or false.",
    "For early_voting_available, early_voting_start_date_rule, and early_voting_end_date_rule, start with the NCSL early in-person voting reference URL in Evidence URLs (https://www.ncsl.org/elections-and-campaigns/early-in-person-voting).",
    "If early_voting_available is false, set early_voting_start_date_rule and early_voting_end_date_rule to null.",
    "If early_voting_available is true, set both early_voting_start_date_rule and early_voting_end_date_rule to short plain-language sentences (not URLs).",
    "online_registration_available must be boolean true or false.",
    "If online_registration_available is false, set online_registration_deadline_rule to null.",
    "online_registration_deadline_rule must be a short plain-language sentence (not URL) when online registration is available; otherwise null.",
    "in_person_registration_deadline_rule must be a short plain-language sentence (not URL).",
    "For in_person_registration_deadline_rule, start with the Vote.gov state registration reference URL in Evidence URLs (https://vote.gov/register/<state-name-lowercase>).",
    "For online_registration_available and online_registration_deadline_rule, start with the Vote.gov state registration reference URL in Evidence URLs (https://vote.gov/register/<state-name-lowercase>).",
    "You may use additional sources beyond that reference URL when needed; it is a starting point, not a restriction.",
    "mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, early_voting_start_date_rule, early_voting_end_date_rule, polling_hours, and id_requirements must be plain-language text summaries, not URLs.",
    "For mail-voting fields, start with the Vote.gov state registration reference URL in Evidence URLs (https://vote.gov/register/<state-name-lowercase>) before expanding to additional sources.",
    "For mail_ballot_return_deadline_rule: include at least one concrete state rule detail.",
    "For polling_hours: include statewide opening/closing times when available; otherwise explicitly state that hours vary by county/precinct.",
    "For id_requirements, output exactly one value from this set and nothing else:",
    "\"Strict photo ID\", \"Strict non-photo ID\", \"Non-strict photo ID\", \"Non-strict, non-photo ID\", \"No document required to vote\".",
    "For id_requirements, start with the NCSL voter ID reference URL in Evidence URLs (https://www.ncsl.org/elections-and-campaigns/voter-id).",
    "For full-sentence summary fields (mail_ballot_request_deadline_rule when present, mail_ballot_return_deadline_rule when present, early_voting_start_date_rule when present, early_voting_end_date_rule when present, polling_hours, id_requirements, in_person_registration_deadline_rule), provide at least one citation each.",
    "For mail_voting_available, mail_ballot_return_deadline_type when present, early_voting_available, same_day_registration_available, online_registration_available, and online_registration_deadline_rule, provide at least one citation each.",
    "sources.id_requirements must include at least one citation that directly supports the chosen id_requirements category.",
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
    "Evidence URLs:",
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
