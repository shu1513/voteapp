import type {
  EnrichStateResourcesConfig,
  EnrichStateResourcesInput,
  PromptVariant,
  ProviderGenerateResult,
} from "../types.js";
import { buildRetryFeedbackPromptLines } from "../retryFeedback.js";

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
    "mail_voting_available must be boolean true or false.",
    "If mail_voting_available is false, set mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, and mail_ballot_return_deadline_type to null.",
    "If mail_voting_available is true, set mail_ballot_return_deadline_rule and mail_ballot_return_deadline_type (postmarked_by or received_by).",
    "online_registration_available must be boolean true or false.",
    "If online_registration_available is false, set online_registration_deadline_rule to null.",
    "online_registration_deadline_rule must be a short plain-language sentence (not URL) when online registration is available; otherwise null.",
    "For online_registration_available and online_registration_deadline_rule, start with the Vote.gov state registration reference URL in Evidence snippets (https://vote.gov/register/<state-name-lowercase>).",
    "You may use additional sources beyond that reference URL when needed; it is a starting point, not a restriction.",
    "mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, polling_hours, and id_requirements must be plain-language text summaries, not URLs.",
    "For mail-voting fields, start with the Vote.gov state registration reference URL in Evidence snippets (https://vote.gov/register/<state-name-lowercase>) before expanding to additional sources.",
    "For mail_ballot_return_deadline_rule: include at least one concrete state rule detail.",
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
  const retryFeedbackLines = buildRetryFeedbackPromptLines(input.retryFeedback);
  const prompt = buildPrompt(input, retryFeedbackLines);
  const promptDebugMeta = {
    provider_prompt_variant: input.promptVariant ?? "default",
    provider_prompt_has_retry_feedback: retryFeedbackLines.length > 0,
    provider_prompt_retry_feedback_snapshot: retryFeedbackLines.length > 0 ? retryFeedbackLines.join("\n") : null,
  } as const;

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
            content: prompt,
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
          reason: `Claude temporary error ${response.status}: ${bodyText}`,
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
        reason: `Claude request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          ...promptDebugMeta,
          provider_response_text: trimDebugText(bodyText),
        },
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
