import {
  STATE_RESOURCE_EARLY_VOTING_END_DATE_RULE_MAX_LENGTH,
  STATE_RESOURCE_EARLY_VOTING_START_DATE_RULE_MAX_LENGTH,
  STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_MAX_LENGTH,
  isValidStateResourceIdRequirementValue,
  STATE_RESOURCE_MAIL_BALLOT_REQUEST_DEADLINE_MAX_LENGTH,
  STATE_RESOURCE_MAIL_BALLOT_RETURN_DEADLINE_MAX_LENGTH,
  STATE_RESOURCE_ONLINE_REGISTRATION_DEADLINE_MAX_LENGTH,
  STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH,
  STATE_RESOURCE_REQUIRED_BOOLEAN_FIELDS,
  STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
  STATE_RESOURCE_SOURCE_FIELDS,
} from "../contracts/stateResourceEnrichmentContract.js";
import type { StateResourcePayload, StateResourceSources } from "../types/stateResource.js";
import { isUrlOnlyText } from "../utils/isUrlOnlyText.js";

const AI_REQUIRED_TEXT_FIELDS: ReadonlyArray<keyof StateResourcePayload> = [
  "polling_place_url",
  "polling_hours",
  "id_requirements",
  "in_person_registration_deadline_rule",
];

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

function isValidMailDeadlineType(value: unknown): value is "postmarked_by" | "received_by" {
  return value === "postmarked_by" || value === "received_by";
}

function validateStateSpecificFieldQuality(payload: StateResourcePayload): string | null {
  const pollingHoursHasTime = /\b\d{1,2}(:\d{2})?\s?(a\.?m\.?|p\.?m\.?)\b/i.test(payload.polling_hours);
  const pollingHoursHasVariance = hasAnyKeyword(payload.polling_hours, ["varies", "county", "precinct"]);
  if (!pollingHoursHasTime && !pollingHoursHasVariance) {
    return "polling_hours must include concrete opening/closing times or explicit county/precinct variance";
  }

  if (!isValidStateResourceIdRequirementValue(payload.id_requirements)) {
    return "id_requirements must be one of the allowed ID requirement categories";
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

  for (const key of AI_REQUIRED_TEXT_FIELDS) {
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
  if (!Object.hasOwn(input, "mail_ballot_request_deadline_rule")) {
    return {
      ok: false,
      reason: "Missing required field: mail_ballot_request_deadline_rule",
      errorCode: "MISSING_REQUIRED_FIELDS",
    };
  }
  if (!(input.mail_ballot_request_deadline_rule === null || isNonEmptyString(input.mail_ballot_request_deadline_rule))) {
    return {
      ok: false,
      reason: "mail_ballot_request_deadline_rule must be null or a non-empty string",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (!Object.hasOwn(input, "mail_ballot_return_deadline_rule")) {
    return {
      ok: false,
      reason: "Missing required field: mail_ballot_return_deadline_rule",
      errorCode: "MISSING_REQUIRED_FIELDS",
    };
  }
  if (!(input.mail_ballot_return_deadline_rule === null || isNonEmptyString(input.mail_ballot_return_deadline_rule))) {
    return {
      ok: false,
      reason: "mail_ballot_return_deadline_rule must be null or a non-empty string",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (!Object.hasOwn(input, "mail_ballot_return_deadline_type")) {
    return {
      ok: false,
      reason: "Missing required field: mail_ballot_return_deadline_type",
      errorCode: "MISSING_REQUIRED_FIELDS",
    };
  }
  if (!(input.mail_ballot_return_deadline_type === null || isValidMailDeadlineType(input.mail_ballot_return_deadline_type))) {
    return {
      ok: false,
      reason: "mail_ballot_return_deadline_type must be null, postmarked_by, or received_by",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (!Object.hasOwn(input, "early_voting_start_date_rule")) {
    return {
      ok: false,
      reason: "Missing required field: early_voting_start_date_rule",
      errorCode: "MISSING_REQUIRED_FIELDS",
    };
  }
  if (!(input.early_voting_start_date_rule === null || isNonEmptyString(input.early_voting_start_date_rule))) {
    return {
      ok: false,
      reason: "early_voting_start_date_rule must be null or a non-empty string",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (!Object.hasOwn(input, "early_voting_end_date_rule")) {
    return {
      ok: false,
      reason: "Missing required field: early_voting_end_date_rule",
      errorCode: "MISSING_REQUIRED_FIELDS",
    };
  }
  if (!(input.early_voting_end_date_rule === null || isNonEmptyString(input.early_voting_end_date_rule))) {
    return {
      ok: false,
      reason: "early_voting_end_date_rule must be null or a non-empty string",
      errorCode: "SCHEMA_MISMATCH",
    };
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
  if (input.mail_voting_available === false) {
    input.mail_ballot_request_deadline_rule = null;
    input.mail_ballot_return_deadline_rule = null;
    input.mail_ballot_return_deadline_type = null;
  }
  if (input.early_voting_available === false) {
    input.early_voting_start_date_rule = null;
    input.early_voting_end_date_rule = null;
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

  const payload: StateResourcePayload = {
    state_fips: isNonEmptyString(input.state_fips) ? input.state_fips.trim() : "",
    state_abbreviation: isNonEmptyString(input.state_abbreviation) ? input.state_abbreviation.trim() : "",
    state_name: isNonEmptyString(input.state_name) ? input.state_name.trim() : "",
    polling_place_url: (input.polling_place_url as string).trim(),
    voter_registration_url: STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
    mail_voting_available: input.mail_voting_available as boolean,
    mail_ballot_request_deadline_rule:
      input.mail_ballot_request_deadline_rule === null ? null : (input.mail_ballot_request_deadline_rule as string).trim(),
    mail_ballot_return_deadline_rule:
      input.mail_ballot_return_deadline_rule === null ? null : (input.mail_ballot_return_deadline_rule as string).trim(),
    mail_ballot_return_deadline_type:
      input.mail_ballot_return_deadline_type === null
        ? null
        : (input.mail_ballot_return_deadline_type as "postmarked_by" | "received_by"),
    early_voting_available: input.early_voting_available as boolean,
    early_voting_start_date_rule:
      input.early_voting_start_date_rule === null ? null : (input.early_voting_start_date_rule as string).trim(),
    early_voting_end_date_rule:
      input.early_voting_end_date_rule === null ? null : (input.early_voting_end_date_rule as string).trim(),
    polling_hours: (input.polling_hours as string).trim(),
    id_requirements: (input.id_requirements as string).trim(),
    same_day_registration_available: input.same_day_registration_available as boolean,
    online_registration_available: input.online_registration_available as boolean,
    online_registration_deadline_rule:
      input.online_registration_deadline_rule === null ? null : (input.online_registration_deadline_rule as string).trim(),
    in_person_registration_deadline_rule: (input.in_person_registration_deadline_rule as string).trim(),
    sources: sanitizedSources,
  };

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
  if (payload.mail_voting_available && payload.mail_ballot_return_deadline_rule === null) {
    return {
      ok: false,
      reason: "mail_ballot_return_deadline_rule must be provided when mail_voting_available is true",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (payload.mail_voting_available && payload.mail_ballot_return_deadline_type === null) {
    return {
      ok: false,
      reason: "mail_ballot_return_deadline_type must be provided when mail_voting_available is true",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (payload.early_voting_available && payload.early_voting_start_date_rule === null) {
    return {
      ok: false,
      reason: "early_voting_start_date_rule must be provided when early_voting_available is true",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (payload.early_voting_available && payload.early_voting_end_date_rule === null) {
    return {
      ok: false,
      reason: "early_voting_end_date_rule must be provided when early_voting_available is true",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (!payload.early_voting_available) {
    if (payload.early_voting_start_date_rule !== null) {
      return {
        ok: false,
        reason: "early_voting_start_date_rule must be null when early_voting_available is false",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (payload.early_voting_end_date_rule !== null) {
      return {
        ok: false,
        reason: "early_voting_end_date_rule must be null when early_voting_available is false",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
  }
  if (!payload.mail_voting_available) {
    if (payload.mail_ballot_request_deadline_rule !== null) {
      return {
        ok: false,
        reason: "mail_ballot_request_deadline_rule must be null when mail_voting_available is false",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (payload.mail_ballot_return_deadline_rule !== null) {
      return {
        ok: false,
        reason: "mail_ballot_return_deadline_rule must be null when mail_voting_available is false",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (payload.mail_ballot_return_deadline_type !== null) {
      return {
        ok: false,
        reason: "mail_ballot_return_deadline_type must be null when mail_voting_available is false",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
  }

  if (
    payload.mail_ballot_request_deadline_rule !== null &&
    payload.mail_ballot_request_deadline_rule.length > STATE_RESOURCE_MAIL_BALLOT_REQUEST_DEADLINE_MAX_LENGTH
  ) {
    return {
      ok: false,
      reason: `mail_ballot_request_deadline_rule must be ${STATE_RESOURCE_MAIL_BALLOT_REQUEST_DEADLINE_MAX_LENGTH} characters or fewer`,
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (
    payload.mail_ballot_return_deadline_rule !== null &&
    payload.mail_ballot_return_deadline_rule.length > STATE_RESOURCE_MAIL_BALLOT_RETURN_DEADLINE_MAX_LENGTH
  ) {
    return {
      ok: false,
      reason: `mail_ballot_return_deadline_rule must be ${STATE_RESOURCE_MAIL_BALLOT_RETURN_DEADLINE_MAX_LENGTH} characters or fewer`,
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (
    payload.early_voting_start_date_rule !== null &&
    payload.early_voting_start_date_rule.length > STATE_RESOURCE_EARLY_VOTING_START_DATE_RULE_MAX_LENGTH
  ) {
    return {
      ok: false,
      reason: `early_voting_start_date_rule must be ${STATE_RESOURCE_EARLY_VOTING_START_DATE_RULE_MAX_LENGTH} characters or fewer`,
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (
    payload.early_voting_end_date_rule !== null &&
    payload.early_voting_end_date_rule.length > STATE_RESOURCE_EARLY_VOTING_END_DATE_RULE_MAX_LENGTH
  ) {
    return {
      ok: false,
      reason: `early_voting_end_date_rule must be ${STATE_RESOURCE_EARLY_VOTING_END_DATE_RULE_MAX_LENGTH} characters or fewer`,
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (
    payload.mail_ballot_return_deadline_type !== null &&
    !isValidMailDeadlineType(payload.mail_ballot_return_deadline_type)
  ) {
    return {
      ok: false,
      reason: "mail_ballot_return_deadline_type must be postmarked_by or received_by",
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
  if (
    payload.in_person_registration_deadline_rule.length > STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_MAX_LENGTH
  ) {
    return {
      ok: false,
      reason: `in_person_registration_deadline_rule must be ${STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_MAX_LENGTH} characters or fewer`,
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  if (payload.mail_ballot_request_deadline_rule !== null && isUrlOnlyText(payload.mail_ballot_request_deadline_rule)) {
    return {
      ok: false,
      reason: "mail_ballot_request_deadline_rule must be plain-language text, not a URL",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (payload.mail_ballot_return_deadline_rule !== null && isUrlOnlyText(payload.mail_ballot_return_deadline_rule)) {
    return {
      ok: false,
      reason: "mail_ballot_return_deadline_rule must be plain-language text, not a URL",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (payload.early_voting_start_date_rule !== null && isUrlOnlyText(payload.early_voting_start_date_rule)) {
    return {
      ok: false,
      reason: "early_voting_start_date_rule must be plain-language text, not a URL",
      errorCode: "SCHEMA_MISMATCH",
    };
  }
  if (payload.early_voting_end_date_rule !== null && isUrlOnlyText(payload.early_voting_end_date_rule)) {
    return {
      ok: false,
      reason: "early_voting_end_date_rule must be plain-language text, not a URL",
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
  if (isUrlOnlyText(payload.in_person_registration_deadline_rule)) {
    return {
      ok: false,
      reason: "in_person_registration_deadline_rule must be plain-language text, not a URL",
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
