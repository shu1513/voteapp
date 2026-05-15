import {
  STATE_RESOURCE_EARLY_VOTING_END_DATE_RULE_MAX_LENGTH,
  STATE_RESOURCE_EARLY_VOTING_START_DATE_RULE_MAX_LENGTH,
  STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_MAX_LENGTH,
  isValidStateResourceIdRequirementValue,
  STATE_RESOURCE_MAIL_BALLOT_REQUEST_DEADLINE_MAX_LENGTH,
  STATE_RESOURCE_MAIL_BALLOT_RETURN_DEADLINE_MAX_LENGTH,
  STATE_RESOURCE_ONLINE_REGISTRATION_DEADLINE_MAX_LENGTH,
  STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH,
  STATE_RESOURCE_SOURCE_FIELDS,
} from "../contracts/stateResourceEnrichmentContract.js";
import { parseCanonicalStateResourcePayload } from "../contracts/stateResourcePayloadContract.js";
import type { StateResourcePayload, StateResourceSources } from "../types/stateResource.js";
import {
  getStateResourceFieldGroupConfig,
  type StateResourceFieldGroup,
} from "./stateResourceFieldGroups.js";
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

type GroupParseResult =
  | {
      ok: true;
      payload: Partial<Omit<StateResourcePayload, "sources">> & {
        sources: Partial<StateResourceSources>;
      };
    }
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

function isValidMailDeadlineType(value: unknown): value is "postmarked_by" | "received_by" {
  return value === "postmarked_by" || value === "received_by";
}

function validateStateSpecificFieldQuality(payload: StateResourcePayload): string | null {
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

function parseScopedSources(
  sourcesRaw: unknown,
  requiredSourceKeys: readonly (keyof StateResourceSources)[]
): { ok: true; sources: Partial<StateResourceSources> } | { ok: false; reason: string; errorCode: "MISSING_REQUIRED_FIELDS" | "SCHEMA_MISMATCH" } {
  if (typeof sourcesRaw !== "object" || sourcesRaw === null || Array.isArray(sourcesRaw)) {
    return {
      ok: false,
      reason: "Missing or invalid sources object",
      errorCode: "MISSING_REQUIRED_FIELDS",
    };
  }

  const obj = sourcesRaw as Record<string, unknown>;
  const scopedSources = {} as Partial<StateResourceSources>;
  for (const key of requiredSourceKeys) {
    const citations = obj[key];
    if (!Array.isArray(citations) || citations.length === 0) {
      return {
        ok: false,
        reason: `sources.${key} must be a non-empty array`,
        errorCode: "MISSING_REQUIRED_FIELDS",
      };
    }
    const sanitized: string[] = [];
    for (const citation of citations) {
      const url = sanitizeCitationUrl(citation);
      if (!url) {
        return {
          ok: false,
          reason: `sources.${key} contains invalid citation URLs`,
          errorCode: "SCHEMA_MISMATCH",
        };
      }
      sanitized.push(url);
    }
    scopedSources[key] = sanitized;
  }

  return { ok: true, sources: scopedSources };
}

export function parseStateResourceGroupPayloadFromAi(
  raw: unknown,
  group: StateResourceFieldGroup
): GroupParseResult {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return { ok: false, reason: "AI output must be a JSON object", errorCode: "INVALID_JSON" };
  }

  const input = raw as Record<string, unknown>;
  const groupConfig = getStateResourceFieldGroupConfig(group);
  for (const key of groupConfig.fieldKeys) {
    if (!Object.hasOwn(input, key)) {
      return {
        ok: false,
        reason: `Missing required field: ${key}`,
        errorCode: "MISSING_REQUIRED_FIELDS",
      };
    }
  }

  const sourcesResult = parseScopedSources(input.sources, groupConfig.sourceKeys);
  if (!sourcesResult.ok) {
    return sourcesResult;
  }

  const payload: Partial<Omit<StateResourcePayload, "sources">> & { sources: Partial<StateResourceSources> } = {
    sources: sourcesResult.sources,
  };

  if (group === "polling_place") {
    if (!isNonEmptyString(input.polling_place_url) || !isHttpUrl(input.polling_place_url.trim())) {
      return {
        ok: false,
        reason: "polling_place_url must be a valid http(s) URL",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    payload.polling_place_url = input.polling_place_url.trim();
    return { ok: true, payload };
  }

  if (group === "polling_hours") {
    if (!isNonEmptyString(input.polling_hours)) {
      return { ok: false, reason: "polling_hours must be a non-empty string", errorCode: "SCHEMA_MISMATCH" };
    }
    const pollingHours = input.polling_hours.trim();
    if (pollingHours.length > STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH) {
      return {
        ok: false,
        reason: `polling_hours must be ${STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH} characters or fewer`,
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (isUrlOnlyText(pollingHours)) {
      return { ok: false, reason: "polling_hours must be plain-language text, not a URL", errorCode: "SCHEMA_MISMATCH" };
    }
    payload.polling_hours = pollingHours;
    return { ok: true, payload };
  }

  if (group === "id_requirements") {
    if (!isNonEmptyString(input.id_requirements)) {
      return { ok: false, reason: "id_requirements must be a non-empty string", errorCode: "SCHEMA_MISMATCH" };
    }
    const value = input.id_requirements.trim();
    if (!isValidStateResourceIdRequirementValue(value)) {
      return {
        ok: false,
        reason: "id_requirements must be one of the allowed ID requirement categories",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    payload.id_requirements = value;
    return { ok: true, payload };
  }

  if (group === "same_day_registration") {
    if (typeof input.same_day_registration_available !== "boolean") {
      return {
        ok: false,
        reason: "same_day_registration_available must be boolean",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    payload.same_day_registration_available = input.same_day_registration_available;
    return { ok: true, payload };
  }

  if (group === "online_registration") {
    if (typeof input.online_registration_available !== "boolean") {
      return {
        ok: false,
        reason: "online_registration_available must be boolean",
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
    const rawRule = input.online_registration_deadline_rule;
    if (!(rawRule === null || isNonEmptyString(rawRule))) {
      return {
        ok: false,
        reason: "online_registration_deadline_rule must be null or a non-empty string",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    const normalizedRule = input.online_registration_available ? (rawRule as string | null)?.trim() ?? null : null;
    if (input.online_registration_available && normalizedRule === null) {
      return {
        ok: false,
        reason: "online_registration_deadline_rule must be provided when online_registration_available is true",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (
      normalizedRule !== null &&
      normalizedRule.length > STATE_RESOURCE_ONLINE_REGISTRATION_DEADLINE_MAX_LENGTH
    ) {
      return {
        ok: false,
        reason: `online_registration_deadline_rule must be ${STATE_RESOURCE_ONLINE_REGISTRATION_DEADLINE_MAX_LENGTH} characters or fewer`,
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (normalizedRule !== null && isUrlOnlyText(normalizedRule)) {
      return {
        ok: false,
        reason: "online_registration_deadline_rule must be plain-language text, not a URL",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    payload.online_registration_available = input.online_registration_available;
    payload.online_registration_deadline_rule = normalizedRule;
    return { ok: true, payload };
  }

  if (group === "mail") {
    if (typeof input.mail_voting_available !== "boolean") {
      return {
        ok: false,
        reason: "mail_voting_available must be boolean",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    const requestRaw = input.mail_ballot_request_deadline_rule;
    const returnRaw = input.mail_ballot_return_deadline_rule;
    const typeRaw = input.mail_ballot_return_deadline_type;
    if (!(requestRaw === null || isNonEmptyString(requestRaw))) {
      return {
        ok: false,
        reason: "mail_ballot_request_deadline_rule must be null or a non-empty string",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (!(returnRaw === null || isNonEmptyString(returnRaw))) {
      return {
        ok: false,
        reason: "mail_ballot_return_deadline_rule must be null or a non-empty string",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (!(typeRaw === null || isValidMailDeadlineType(typeRaw))) {
      return {
        ok: false,
        reason: "mail_ballot_return_deadline_type must be null, postmarked_by, or received_by",
        errorCode: "SCHEMA_MISMATCH",
      };
    }

    const normalizedRequest = input.mail_voting_available ? (requestRaw as string | null)?.trim() ?? null : null;
    const normalizedReturn = input.mail_voting_available ? (returnRaw as string | null)?.trim() ?? null : null;
    const normalizedType = input.mail_voting_available ? (typeRaw as "postmarked_by" | "received_by" | null) : null;

    if (input.mail_voting_available && normalizedReturn === null) {
      return {
        ok: false,
        reason: "mail_ballot_return_deadline_rule must be provided when mail_voting_available is true",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (input.mail_voting_available && normalizedType === null) {
      return {
        ok: false,
        reason: "mail_ballot_return_deadline_type must be provided when mail_voting_available is true",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (
      normalizedRequest !== null &&
      normalizedRequest.length > STATE_RESOURCE_MAIL_BALLOT_REQUEST_DEADLINE_MAX_LENGTH
    ) {
      return {
        ok: false,
        reason: `mail_ballot_request_deadline_rule must be ${STATE_RESOURCE_MAIL_BALLOT_REQUEST_DEADLINE_MAX_LENGTH} characters or fewer`,
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (
      normalizedReturn !== null &&
      normalizedReturn.length > STATE_RESOURCE_MAIL_BALLOT_RETURN_DEADLINE_MAX_LENGTH
    ) {
      return {
        ok: false,
        reason: `mail_ballot_return_deadline_rule must be ${STATE_RESOURCE_MAIL_BALLOT_RETURN_DEADLINE_MAX_LENGTH} characters or fewer`,
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (normalizedRequest !== null && isUrlOnlyText(normalizedRequest)) {
      return {
        ok: false,
        reason: "mail_ballot_request_deadline_rule must be plain-language text, not a URL",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (normalizedReturn !== null && isUrlOnlyText(normalizedReturn)) {
      return {
        ok: false,
        reason: "mail_ballot_return_deadline_rule must be plain-language text, not a URL",
        errorCode: "SCHEMA_MISMATCH",
      };
    }

    payload.mail_voting_available = input.mail_voting_available;
    payload.mail_ballot_request_deadline_rule = normalizedRequest;
    payload.mail_ballot_return_deadline_rule = normalizedReturn;
    payload.mail_ballot_return_deadline_type = normalizedType;
    return { ok: true, payload };
  }

  if (group === "early_voting") {
    if (typeof input.early_voting_available !== "boolean") {
      return {
        ok: false,
        reason: "early_voting_available must be boolean",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    const startRaw = input.early_voting_start_date_rule;
    const endRaw = input.early_voting_end_date_rule;
    if (!(startRaw === null || isNonEmptyString(startRaw))) {
      return {
        ok: false,
        reason: "early_voting_start_date_rule must be null or a non-empty string",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (!(endRaw === null || isNonEmptyString(endRaw))) {
      return {
        ok: false,
        reason: "early_voting_end_date_rule must be null or a non-empty string",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    const normalizedStart = input.early_voting_available ? (startRaw as string | null)?.trim() ?? null : null;
    const normalizedEnd = input.early_voting_available ? (endRaw as string | null)?.trim() ?? null : null;
    if (input.early_voting_available && normalizedStart === null) {
      return {
        ok: false,
        reason: "early_voting_start_date_rule must be provided when early_voting_available is true",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (input.early_voting_available && normalizedEnd === null) {
      return {
        ok: false,
        reason: "early_voting_end_date_rule must be provided when early_voting_available is true",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (normalizedStart !== null && normalizedStart.length > STATE_RESOURCE_EARLY_VOTING_START_DATE_RULE_MAX_LENGTH) {
      return {
        ok: false,
        reason: `early_voting_start_date_rule must be ${STATE_RESOURCE_EARLY_VOTING_START_DATE_RULE_MAX_LENGTH} characters or fewer`,
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (normalizedEnd !== null && normalizedEnd.length > STATE_RESOURCE_EARLY_VOTING_END_DATE_RULE_MAX_LENGTH) {
      return {
        ok: false,
        reason: `early_voting_end_date_rule must be ${STATE_RESOURCE_EARLY_VOTING_END_DATE_RULE_MAX_LENGTH} characters or fewer`,
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (normalizedStart !== null && isUrlOnlyText(normalizedStart)) {
      return {
        ok: false,
        reason: "early_voting_start_date_rule must be plain-language text, not a URL",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (normalizedEnd !== null && isUrlOnlyText(normalizedEnd)) {
      return {
        ok: false,
        reason: "early_voting_end_date_rule must be plain-language text, not a URL",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    payload.early_voting_available = input.early_voting_available;
    payload.early_voting_start_date_rule = normalizedStart;
    payload.early_voting_end_date_rule = normalizedEnd;
    return { ok: true, payload };
  }

  if (group === "in_person_registration") {
    if (!isNonEmptyString(input.in_person_registration_deadline_rule)) {
      return {
        ok: false,
        reason: "in_person_registration_deadline_rule must be a non-empty string",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    const rule = input.in_person_registration_deadline_rule.trim();
    if (rule.length > STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_MAX_LENGTH) {
      return {
        ok: false,
        reason: `in_person_registration_deadline_rule must be ${STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_MAX_LENGTH} characters or fewer`,
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    if (isUrlOnlyText(rule)) {
      return {
        ok: false,
        reason: "in_person_registration_deadline_rule must be plain-language text, not a URL",
        errorCode: "SCHEMA_MISMATCH",
      };
    }
    payload.in_person_registration_deadline_rule = rule;
    return { ok: true, payload };
  }

  return {
    ok: false,
    reason: `Unsupported field group: ${group}`,
    errorCode: "SCHEMA_MISMATCH",
  };
}

/**
 * Parses and validates unknown AI output into strict StateResourcePayload.
 */
export function parseStateResourcePayloadFromAi(raw: unknown): ParseResult {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return { ok: false, reason: "AI output must be a JSON object", errorCode: "INVALID_JSON" };
  }

  const input = raw as Record<string, unknown>;

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

  const parsedCanonical = parseCanonicalStateResourcePayload(input);
  if (!parsedCanonical.ok) {
    const missingLikeReason =
      parsedCanonical.reason.includes("must be a non-empty string") ||
      parsedCanonical.reason.includes("must be boolean") ||
      parsedCanonical.reason.includes("must be present") ||
      parsedCanonical.reason.includes("must be a non-empty array") ||
      parsedCanonical.reason.includes("must be an object");
    return {
      ok: false,
      reason: parsedCanonical.reason,
      errorCode: missingLikeReason ? "MISSING_REQUIRED_FIELDS" : "SCHEMA_MISMATCH",
    };
  }

  const payload = parsedCanonical.payload;

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
