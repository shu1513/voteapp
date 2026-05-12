import {
  STATE_RESOURCE_ABBREVIATION_REGEX,
  STATE_RESOURCE_FIPS_REGEX,
  STATE_RESOURCE_ONLINE_REGISTRATION_DEADLINE_MAX_LENGTH,
  STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH,
  STATE_RESOURCE_REQUIRED_BOOLEAN_FIELDS,
  STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
  STATE_RESOURCE_REQUIRED_TEXT_FIELDS,
  STATE_RESOURCE_SOURCE_FIELDS,
  STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH,
} from "../contracts/stateResourceEnrichmentContract.js";
import type { StateResourcePayload, StateResourceSources } from "../types/stateResource.js";
import { isUrlOnlyText } from "../utils/isUrlOnlyText.js";

type ParseResult =
  | { ok: true; payload: StateResourcePayload }
  | { ok: false; reason: string; errorCode: "INVALID_JSON" | "MISSING_REQUIRED_FIELDS" | "SCHEMA_MISMATCH" };

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function hasAnyKeyword(text: string, keywords: string[]): boolean {
  const lower = text.toLowerCase();
  return keywords.some((keyword) => lower.includes(keyword));
}

function hasClearIdRequirementStatement(text: string): boolean {
  const lower = text.toLowerCase();
  const patterns = [
    /\b(?:voter\s+)?id\s+is\s+required\b/,
    /\b(?:voter\s+)?id\s+required\b/,
    /\brequires?\s+(?:a\s+)?(?:valid\s+)?(?:photo\s+)?id\b/,
    /\b(?:voter\s+)?id\s+is\s+not\s+required\b/,
    /\bno\s+(?:photo\s+)?id\s+(?:is\s+)?required\b/,
    /\bdo(?:es)?\s+not\s+require\s+(?:a\s+)?(?:photo\s+)?(?:voter\s+)?id\b/,
    /\b(?:no|without)\s+(?:voter\s+)?id\b/,
    /\bidentification\s+is\s+(?:not\s+)?required\b/,
  ];

  return patterns.some((pattern) => pattern.test(lower));
}

function looksLikeMockBoilerplate(field: "vote_by_mail_info" | "polling_hours" | "id_requirements", text: string): boolean {
  const normalized = text.trim().toLowerCase();
  if (
    field === "vote_by_mail_info" &&
    /^(?:[a-z .'-]+ voters can request and return|voters can request and return) vote-by-mail ballots based on state deadlines and local election rules\.?$/.test(
      normalized
    )
  ) {
    return true;
  }

  if (
    field === "polling_hours" &&
    /^polling locations usually open and close at posted local hours on election day\.?$/.test(normalized)
  ) {
    return true;
  }

  if (
    field === "id_requirements" &&
    /^(?:[a-z .'-]+ voter id requirements depend on|voter id requirements depend on) election type and local\/state rules\.?$/.test(normalized)
  ) {
    return true;
  }

  return false;
}

function validateStateSpecificFieldQuality(payload: StateResourcePayload): string | null {
  if (looksLikeMockBoilerplate("vote_by_mail_info", payload.vote_by_mail_info)) {
    return "vote_by_mail_info is generic boilerplate; include state-specific legal details";
  }
  if (looksLikeMockBoilerplate("polling_hours", payload.polling_hours)) {
    return "polling_hours is generic boilerplate; include state-specific hours detail";
  }
  if (looksLikeMockBoilerplate("id_requirements", payload.id_requirements)) {
    return "id_requirements is generic boilerplate; include state-specific ID policy";
  }

  const voteByMailKeywords = ["deadline", "postmark", "request", "return", "received", "drop box", "mail", "absentee"];
  if (!hasAnyKeyword(payload.vote_by_mail_info, voteByMailKeywords)) {
    return "vote_by_mail_info must include at least one concrete vote-by-mail rule detail";
  }

  const pollingHoursHasTime = /\b\d{1,2}(:\d{2})?\s?(a\.?m\.?|p\.?m\.?)\b/i.test(payload.polling_hours);
  const pollingHoursHasVariance = hasAnyKeyword(payload.polling_hours, ["varies", "county", "precinct"]);
  if (!pollingHoursHasTime && !pollingHoursHasVariance) {
    return "polling_hours must include concrete opening/closing times or explicit county/precinct variance";
  }

  if (!hasClearIdRequirementStatement(payload.id_requirements)) {
    return "id_requirements must clearly state whether voter identification is required";
  }

  return null;
}

function sanitizeCitationUrl(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return isHttpUrl(trimmed) ? trimmed : null;
  }

  // Backward-compatible read path for older object citations.
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    const item = value as Record<string, unknown>;
    if (isNonEmptyString(item.source_url)) {
      const trimmed = item.source_url.trim();
      return isHttpUrl(trimmed) ? trimmed : null;
    }
  }

  return null;
}

/**
 * Parses and validates unknown AI output into strict StateResourcePayload.
 */
export function parseStateResourcePayloadFromAi(raw: unknown): ParseResult {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return { ok: false, reason: "AI output must be a JSON object", errorCode: "INVALID_JSON" };
  }

  const input = raw as Record<string, unknown>;

  for (const key of STATE_RESOURCE_REQUIRED_TEXT_FIELDS) {
    if (!isNonEmptyString(input[key])) {
      return {
        ok: false,
        reason: `Missing required field: ${key}`,
        errorCode: "MISSING_REQUIRED_FIELDS",
      };
    }
  }

  for (const key of STATE_RESOURCE_REQUIRED_BOOLEAN_FIELDS) {
    if (typeof input[key] !== "boolean") {
      return {
        ok: false,
        reason: `Missing required boolean field: ${key}`,
        errorCode: "MISSING_REQUIRED_FIELDS",
      };
    }
  }

  if (!Object.hasOwn(input, "online_registration_deadline_rule")) {
    return {
      ok: false,
      reason: "Missing required field: online_registration_deadline_rule",
      errorCode: "MISSING_REQUIRED_FIELDS",
    };
  }
  if (!(input.online_registration_deadline_rule === null || isNonEmptyString(input.online_registration_deadline_rule))) {
    return {
      ok: false,
      reason: "online_registration_deadline_rule must be null or a non-empty string",
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  // Canonical normalization: if online registration is not available,
  // force deadline rule to null instead of failing.
  if (input.online_registration_available === false) {
    input.online_registration_deadline_rule = null;
  }

  if (typeof input.sources !== "object" || input.sources === null || Array.isArray(input.sources)) {
    return {
      ok: false,
      reason: "Missing or invalid sources object",
      errorCode: "MISSING_REQUIRED_FIELDS",
    };
  }

  const sourcesObj = input.sources as Record<string, unknown>;
  const sanitizedSources = {} as StateResourceSources;
  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const citations = sourcesObj[key];
    if (!Array.isArray(citations) || citations.length === 0) {
      return {
        ok: false,
        reason: `sources.${key} must be a non-empty array`,
        errorCode: "MISSING_REQUIRED_FIELDS",
      };
    }

    const sanitizedBucket: string[] = [];
    for (const citation of citations) {
      const sanitized = sanitizeCitationUrl(citation);
      if (!sanitized) {
        return {
          ok: false,
          reason: `sources.${key} contains invalid citation URLs`,
          errorCode: "SCHEMA_MISMATCH",
        };
      }
      sanitizedBucket.push(sanitized);
    }

    sanitizedSources[key] = sanitizedBucket;
  }
  sanitizedSources.voter_registration_url = [STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL];

  const payload: StateResourcePayload = {
    state_fips: (input.state_fips as string).trim(),
    state_abbreviation: (input.state_abbreviation as string).trim(),
    state_name: (input.state_name as string).trim(),
    polling_place_url: (input.polling_place_url as string).trim(),
    voter_registration_url: STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
    vote_by_mail_info: (input.vote_by_mail_info as string).trim(),
    polling_hours: (input.polling_hours as string).trim(),
    id_requirements: (input.id_requirements as string).trim(),
    online_registration_available: input.online_registration_available as boolean,
    online_registration_deadline_rule:
      input.online_registration_deadline_rule === null ? null : (input.online_registration_deadline_rule as string).trim(),
    sources: sanitizedSources,
  };

  if (!STATE_RESOURCE_FIPS_REGEX.test(payload.state_fips)) {
    return {
      ok: false,
      reason: "state_fips must be exactly two digits",
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (!STATE_RESOURCE_ABBREVIATION_REGEX.test(payload.state_abbreviation)) {
    return {
      ok: false,
      reason: "state_abbreviation must be two uppercase letters",
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (!isHttpUrl(payload.polling_place_url)) {
    return { ok: false, reason: "polling_place_url must be a valid http(s) URL", errorCode: "SCHEMA_MISMATCH" };
  }

  if (!isHttpUrl(payload.voter_registration_url)) {
    return {
      ok: false,
      reason: "voter_registration_url must be a valid http(s) URL",
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (payload.online_registration_available && payload.online_registration_deadline_rule === null) {
    return {
      ok: false,
      reason: "online_registration_deadline_rule must be provided when online_registration_available is true",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (payload.vote_by_mail_info.length > STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH) {
    return {
      ok: false,
      reason: `vote_by_mail_info must be ${STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH} characters or fewer`,
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (payload.polling_hours.length > STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH) {
    return {
      ok: false,
      reason: `polling_hours must be ${STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH} characters or fewer`,
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (
    payload.online_registration_deadline_rule !== null &&
    payload.online_registration_deadline_rule.length > STATE_RESOURCE_ONLINE_REGISTRATION_DEADLINE_MAX_LENGTH
  ) {
    return {
      ok: false,
      reason: `online_registration_deadline_rule must be ${STATE_RESOURCE_ONLINE_REGISTRATION_DEADLINE_MAX_LENGTH} characters or fewer`,
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (isUrlOnlyText(payload.vote_by_mail_info)) {
    return {
      ok: false,
      reason: "vote_by_mail_info must be plain-language text, not a URL",
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (isUrlOnlyText(payload.polling_hours)) {
    return {
      ok: false,
      reason: "polling_hours must be plain-language text, not a URL",
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (isUrlOnlyText(payload.id_requirements)) {
    return {
      ok: false,
      reason: "id_requirements must be plain-language text, not a URL",
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (payload.online_registration_deadline_rule !== null && isUrlOnlyText(payload.online_registration_deadline_rule)) {
    return {
      ok: false,
      reason: "online_registration_deadline_rule must be plain-language text, not a URL",
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  const qualityReason = validateStateSpecificFieldQuality(payload);
  if (qualityReason) {
    return {
      ok: false,
      reason: qualityReason,
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  return { ok: true, payload };
}
