import type { StateResourcePayload, StateResourceSources } from "../types/stateResource.js";

/**
 * Version tag for producer draft payload shape.
 */
export const STATE_RESOURCE_DRAFT_SCHEMA_VERSION = "state_resources_draft_v1" as const;

/**
 * Version tag for the state_resources enrichment contract.
 * Keep this stable for one schema shape; bump only on breaking payload changes.
 */
export const STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION = "state_resources_enrichment_v1" as const;

/**
 * Required text fields that an enriched state_resources payload must include.
 */
export const STATE_RESOURCE_REQUIRED_TEXT_FIELDS = [
  "state_fips",
  "state_abbreviation",
  "state_name",
  "polling_place_url",
  "voter_registration_url",
  "vote_by_mail_info",
  "polling_hours",
  "id_requirements",
] as const satisfies ReadonlyArray<keyof StateResourcePayload>;

/**
 * Required per-field source buckets in state_resources.sources.
 */
export const STATE_RESOURCE_SOURCE_FIELDS = [
  "polling_place_url",
  "voter_registration_url",
  "vote_by_mail_info",
  "polling_hours",
  "id_requirements",
] as const satisfies ReadonlyArray<keyof StateResourceSources>;

/**
 * Keys that identify producer drafts (not yet AI-enriched).
 */
export const STATE_RESOURCE_DRAFT_MARKER_FIELDS = [
  "census_source_url",
  "seed_sources",
  "allow_open_web_research",
] as const;

export const STATE_RESOURCE_FIPS_REGEX = /^[0-9]{2}$/;
export const STATE_RESOURCE_ABBREVIATION_REGEX = /^[A-Z]{2}$/;

export const STATE_RESOURCE_VOTE_BY_MAIL_MAX_LENGTH = 4000;
export const STATE_RESOURCE_POLLING_HOURS_MAX_LENGTH = 1000;
