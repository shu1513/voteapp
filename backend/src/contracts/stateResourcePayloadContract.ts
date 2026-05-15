import {
  STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
  isValidStateResourceIdRequirementValue,
  STATE_RESOURCE_REQUIRED_BOOLEAN_FIELDS,
  STATE_RESOURCE_REQUIRED_TEXT_FIELDS,
  STATE_RESOURCE_SOURCE_FIELDS,
} from "./stateResourceEnrichmentContract.js";
import type { StateResourcePayload, StateResourceSources } from "../types/stateResource.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

type ParseResult =
  | { ok: true; payload: StateResourcePayload }
  | { ok: false; reason: string };

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeCitationUrl(value: unknown): string | null {
  if (typeof value === "string") {
    return normalizeHttpUrl(value);
  }

  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    const item = value as Record<string, unknown>;
    if (isNonEmptyString(item.source_url)) {
      return normalizeHttpUrl(item.source_url);
    }
  }

  return null;
}

/**
 * Canonical parser for full state_resources payload shape used by staging validator/writer paths.
 * This intentionally performs structure + consistency checks only.
 */
export function parseCanonicalStateResourcePayload(payload: unknown): ParseResult {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  for (const key of STATE_RESOURCE_REQUIRED_TEXT_FIELDS) {
    if (!isNonEmptyString(input[key])) {
      return { ok: false, reason: `payload.${key} must be a non-empty string` };
    }
  }

  for (const key of STATE_RESOURCE_REQUIRED_BOOLEAN_FIELDS) {
    if (typeof input[key] !== "boolean") {
      return { ok: false, reason: `payload.${key} must be boolean` };
    }
  }

  if (!Object.hasOwn(input, "online_registration_deadline_rule")) {
    return { ok: false, reason: "payload.online_registration_deadline_rule must be present (string or null)" };
  }
  if (!(input.online_registration_deadline_rule === null || isNonEmptyString(input.online_registration_deadline_rule))) {
    return { ok: false, reason: "payload.online_registration_deadline_rule must be null or non-empty string" };
  }
  if (input.online_registration_available === true && input.online_registration_deadline_rule === null) {
    return {
      ok: false,
      reason: "payload.online_registration_deadline_rule must be string when online_registration_available=true",
    };
  }
  if (input.online_registration_available === false && input.online_registration_deadline_rule !== null) {
    return {
      ok: false,
      reason: "payload.online_registration_deadline_rule must be null when online_registration_available=false",
    };
  }

  if (!Object.hasOwn(input, "mail_ballot_request_deadline_rule")) {
    return { ok: false, reason: "payload.mail_ballot_request_deadline_rule must be present (string or null)" };
  }
  if (!(input.mail_ballot_request_deadline_rule === null || isNonEmptyString(input.mail_ballot_request_deadline_rule))) {
    return { ok: false, reason: "payload.mail_ballot_request_deadline_rule must be null or non-empty string" };
  }
  if (!Object.hasOwn(input, "mail_ballot_return_deadline_rule")) {
    return { ok: false, reason: "payload.mail_ballot_return_deadline_rule must be present (string or null)" };
  }
  if (!(input.mail_ballot_return_deadline_rule === null || isNonEmptyString(input.mail_ballot_return_deadline_rule))) {
    return { ok: false, reason: "payload.mail_ballot_return_deadline_rule must be null or non-empty string" };
  }
  if (!Object.hasOwn(input, "mail_ballot_return_deadline_type")) {
    return { ok: false, reason: "payload.mail_ballot_return_deadline_type must be present (postmarked_by|received_by|null)" };
  }
  const mailType = input.mail_ballot_return_deadline_type;
  if (!(mailType === null || mailType === "postmarked_by" || mailType === "received_by")) {
    return { ok: false, reason: "payload.mail_ballot_return_deadline_type must be postmarked_by, received_by, or null" };
  }

  if (!Object.hasOwn(input, "early_voting_start_date_rule")) {
    return { ok: false, reason: "payload.early_voting_start_date_rule must be present (string or null)" };
  }
  if (!(input.early_voting_start_date_rule === null || isNonEmptyString(input.early_voting_start_date_rule))) {
    return { ok: false, reason: "payload.early_voting_start_date_rule must be null or non-empty string" };
  }
  if (!Object.hasOwn(input, "early_voting_end_date_rule")) {
    return { ok: false, reason: "payload.early_voting_end_date_rule must be present (string or null)" };
  }
  if (!(input.early_voting_end_date_rule === null || isNonEmptyString(input.early_voting_end_date_rule))) {
    return { ok: false, reason: "payload.early_voting_end_date_rule must be null or non-empty string" };
  }
  if (input.mail_voting_available === true && input.mail_ballot_return_deadline_rule === null) {
    return {
      ok: false,
      reason: "payload.mail_ballot_return_deadline_rule must be string when mail_voting_available=true",
    };
  }
  if (input.mail_voting_available === true && input.mail_ballot_return_deadline_type === null) {
    return {
      ok: false,
      reason: "payload.mail_ballot_return_deadline_type must be set when mail_voting_available=true",
    };
  }
  if (input.mail_voting_available === false) {
    if (input.mail_ballot_request_deadline_rule !== null) {
      return { ok: false, reason: "payload.mail_ballot_request_deadline_rule must be null when mail_voting_available=false" };
    }
    if (input.mail_ballot_return_deadline_rule !== null) {
      return { ok: false, reason: "payload.mail_ballot_return_deadline_rule must be null when mail_voting_available=false" };
    }
    if (input.mail_ballot_return_deadline_type !== null) {
      return { ok: false, reason: "payload.mail_ballot_return_deadline_type must be null when mail_voting_available=false" };
    }
  }
  if (input.early_voting_available === true && input.early_voting_start_date_rule === null) {
    return {
      ok: false,
      reason: "payload.early_voting_start_date_rule must be string when early_voting_available=true",
    };
  }
  if (input.early_voting_available === true && input.early_voting_end_date_rule === null) {
    return {
      ok: false,
      reason: "payload.early_voting_end_date_rule must be string when early_voting_available=true",
    };
  }
  if (input.early_voting_available === false) {
    if (input.early_voting_start_date_rule !== null) {
      return { ok: false, reason: "payload.early_voting_start_date_rule must be null when early_voting_available=false" };
    }
    if (input.early_voting_end_date_rule !== null) {
      return { ok: false, reason: "payload.early_voting_end_date_rule must be null when early_voting_available=false" };
    }
  }

  if (typeof input.sources !== "object" || input.sources === null || Array.isArray(input.sources)) {
    return { ok: false, reason: "payload.sources must be an object" };
  }

  const sources = input.sources as Record<string, unknown>;
  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    const citations = sources[key];
    if (!Array.isArray(citations) || citations.length === 0) {
      return { ok: false, reason: `payload.sources.${key} must be a non-empty array` };
    }

    if (!citations.every((citation) => normalizeCitationUrl(citation) !== null)) {
      return { ok: false, reason: `payload.sources.${key} contains invalid citation URLs` };
    }
  }

  const normalizedSources = {} as StateResourceSources;
  for (const key of STATE_RESOURCE_SOURCE_FIELDS) {
    normalizedSources[key] = (sources[key] as unknown[]).map((citation) => normalizeCitationUrl(citation) as string);
  }

  if (!isValidStateResourceIdRequirementValue((input.id_requirements as string).trim())) {
    return {
      ok: false,
      reason: "id_requirements must be one of the allowed ID requirement categories",
    };
  }

  return {
    ok: true,
    payload: {
      state_fips: (input.state_fips as string).trim(),
      state_abbreviation: (input.state_abbreviation as string).trim(),
      state_name: (input.state_name as string).trim(),
      polling_place_url: (input.polling_place_url as string).trim(),
      voter_registration_url: STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL,
      mail_voting_available: input.mail_voting_available as boolean,
      mail_ballot_request_deadline_rule:
        input.mail_ballot_request_deadline_rule === null
          ? null
          : (input.mail_ballot_request_deadline_rule as string).trim(),
      mail_ballot_return_deadline_rule:
        input.mail_ballot_return_deadline_rule === null
          ? null
          : (input.mail_ballot_return_deadline_rule as string).trim(),
      mail_ballot_return_deadline_type:
        input.mail_ballot_return_deadline_type === null
          ? null
          : (input.mail_ballot_return_deadline_type as "postmarked_by" | "received_by"),
      early_voting_available: input.early_voting_available as boolean,
      early_voting_start_date_rule:
        input.early_voting_start_date_rule === null
          ? null
          : (input.early_voting_start_date_rule as string).trim(),
      early_voting_end_date_rule:
        input.early_voting_end_date_rule === null
          ? null
          : (input.early_voting_end_date_rule as string).trim(),
      polling_hours: (input.polling_hours as string).trim(),
      id_requirements: (input.id_requirements as string).trim(),
      same_day_registration_available: input.same_day_registration_available as boolean,
      online_registration_available: input.online_registration_available as boolean,
      online_registration_deadline_rule:
        input.online_registration_deadline_rule === null
          ? null
          : (input.online_registration_deadline_rule as string).trim(),
      in_person_registration_deadline_rule: (input.in_person_registration_deadline_rule as string).trim(),
      sources: normalizedSources,
    },
  };
}
