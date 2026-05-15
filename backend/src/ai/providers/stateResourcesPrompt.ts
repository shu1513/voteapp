import type { EnrichStateResourcesInput, PromptVariant } from "../types.js";
import { buildScopedPrompt } from "./stateResourceScopedOutput.js";

export function buildPromptVariantLines(promptVariant: PromptVariant | undefined): string[] {
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

export function buildStateResourcesPrompt(
  input: EnrichStateResourcesInput,
  retryFeedbackLines: string[]
): string {
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
    "online_registration_available must be boolean true or false.",
    "If online_registration_available is false, set online_registration_deadline_rule to null.",
    "online_registration_deadline_rule must be a short plain-language sentence (not URL) when online registration is available; otherwise null.",
    "in_person_registration_deadline_rule must be a short plain-language sentence (not URL).",
    "For in_person_registration_deadline_rule, start with the Vote.gov state registration reference URL in Evidence URLs (https://vote.gov/register/<state-name-lowercase>).",
    "For online_registration_available and online_registration_deadline_rule, start with the Vote.gov state registration reference URL in Evidence URLs (https://vote.gov/register/<state-name-lowercase>).",
    "You may use additional sources beyond that reference URL when needed; it is a starting point, not a restriction.",
    "mail_voting_available must be boolean true or false.",
    "If mail_voting_available is false, set mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, and mail_ballot_return_deadline_type to null.",
    "If mail_voting_available is true, set mail_ballot_return_deadline_rule and mail_ballot_return_deadline_type (postmarked_by or received_by).",
    "early_voting_available must be boolean true or false.",
    "For early_voting_available, early_voting_start_date_rule, and early_voting_end_date_rule, start with the NCSL early in-person voting reference URL in Evidence URLs (https://www.ncsl.org/elections-and-campaigns/early-in-person-voting).",
    "If early_voting_available is false, set early_voting_start_date_rule and early_voting_end_date_rule to null.",
    "If early_voting_available is true, set both early_voting_start_date_rule and early_voting_end_date_rule to short plain-language sentences (not URLs).",
    "mail_ballot_request_deadline_rule, mail_ballot_return_deadline_rule, early_voting_start_date_rule, and early_voting_end_date_rule must be short plain-language sentences (not URLs) when present.",
    "For mail-voting fields, start with the Vote.gov state registration reference URL in Evidence URLs (https://vote.gov/register/<state-name-lowercase>) before expanding to additional sources.",
    "For mail_ballot_return_deadline_rule: include a concrete state rule detail.",
    "polling_hours and id_requirements must be plain-language text summaries, not URLs.",
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
