import type { StateResourcePayload, StateResourceSources } from "../types/stateResource.js";

/**
 * Version tag for producer draft payload shape.
 */
export const STATE_RESOURCE_DRAFT_SCHEMA_VERSION = "state_resources_draft_v1" as const;

/**
 * Version tag for the state_resources enrichment contract.
 * Keep this stable for one schema shape; bump only on breaking payload changes.
 */
export const STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION = "state_resources_enrichment_v5" as const;

/**
 * Required text fields that an enriched state_resources payload must include.
 */
export const STATE_RESOURCE_REQUIRED_TEXT_FIELDS = [
  "state_fips",
  "state_abbreviation",
  "state_name",
  "polling_place_url",
  "voter_registration_url",
  "polling_hours",
  "id_requirements",
  "in_person_registration_deadline_rule",
] as const satisfies ReadonlyArray<keyof StateResourcePayload>;

/**
 * Required boolean fields in the enriched payload.
 */
export const STATE_RESOURCE_REQUIRED_BOOLEAN_FIELDS = [
  "mail_voting_available",
  "early_voting_available",
  "same_day_registration_available",
  "online_registration_available",
] as const satisfies ReadonlyArray<keyof StateResourcePayload>;

/**
 * Required per-field source buckets in state_resources.sources.
 */
export const STATE_RESOURCE_SOURCE_FIELDS = [
  "polling_place_url",
  "mail_voting_available",
  "mail_ballot_request_url",
  "mail_ballot_request_type",
  "mail_ballot_request_deadline_rule",
  "mail_ballot_return_deadline_rule",
  "mail_ballot_return_deadline_type",
  "early_voting_available",
  "early_voting_start_date_rule",
  "early_voting_end_date_rule",
  "polling_hours",
  "id_requirements",
  "same_day_registration_available",
  "online_registration_available",
  "online_registration_deadline_rule",
  "in_person_registration_deadline_rule",
] as const satisfies ReadonlyArray<keyof StateResourceSources>;

export const STATE_RESOURCE_FIXED_VOTER_REGISTRATION_URL = "https://vote.gov/register" as const;

/**
 * How a voter obtains a mail/absentee ballot:
 * online_portal -> official online request portal
 * form -> official application form / PDF
 * instructions -> official instructions page (request goes through a local election office)
 * not_required -> automatic vote-by-mail; mail_ballot_request_url is an official explanatory page
 */
export const STATE_RESOURCE_MAIL_BALLOT_REQUEST_TYPES = [
  "online_portal",
  "form",
  "instructions",
  "not_required",
] as const;

export type StateResourceMailBallotRequestType = (typeof STATE_RESOURCE_MAIL_BALLOT_REQUEST_TYPES)[number];

export function isValidStateResourceMailBallotRequestType(
  value: unknown
): value is StateResourceMailBallotRequestType {
  return (
    typeof value === "string" &&
    (STATE_RESOURCE_MAIL_BALLOT_REQUEST_TYPES as readonly string[]).includes(value)
  );
}

export const STATE_RESOURCE_MAIL_BALLOT_REQUEST_URL_MAX_LENGTH = 2048;

export const STATE_RESOURCE_ID_REQUIREMENT_VALUES = [
  "Strict photo ID",
  "Strict non-photo ID",
  "Non-strict photo ID",
  "Non-strict, non-photo ID",
  "No document required to vote",
] as const;

export type StateResourceIdRequirementValue = (typeof STATE_RESOURCE_ID_REQUIREMENT_VALUES)[number];

export function isValidStateResourceIdRequirementValue(value: string): value is StateResourceIdRequirementValue {
  return (STATE_RESOURCE_ID_REQUIREMENT_VALUES as readonly string[]).includes(value);
}

/**
 * Keys that identify producer drafts (not yet AI-enriched).
 */
export const STATE_RESOURCE_DRAFT_MARKER_FIELDS = [
  "census_source_url",
  "seed_sources",
] as const;

export const STATE_RESOURCE_FIPS_REGEX = /^[0-9]{2}$/;
export const STATE_RESOURCE_ABBREVIATION_REGEX = /^[A-Z]{2}$/;

export const STATE_RESOURCE_MAIL_BALLOT_REQUEST_DEADLINE_MAX_LENGTH = 1000;
export const STATE_RESOURCE_MAIL_BALLOT_RETURN_DEADLINE_MAX_LENGTH = 1000;
export const STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH = 1000;
export const STATE_RESOURCE_ID_REQUIREMENTS_MAX_LENGTH = 4000;
export const STATE_RESOURCE_ONLINE_REGISTRATION_DEADLINE_MAX_LENGTH = 1000;
export const STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_MAX_LENGTH = 1000;
export const STATE_RESOURCE_EARLY_VOTING_START_DATE_RULE_MAX_LENGTH = 1000;
export const STATE_RESOURCE_EARLY_VOTING_END_DATE_RULE_MAX_LENGTH = 1000;
export const STATE_RESOURCE_TEXT_MIN_LENGTH = 12;
