export const CENSUS_STATES_API_URL =
  "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*";

export const STATE_RESOURCE_POLLING_REFERENCE_SEEDS = [
  "https://www.vote.org/polling-place-locator/",
  "https://www.nass.org/can-i-vote/find-your-polling-place",
  "https://www.usvotefoundation.org/find-my-polling-place",
] as const;

export const STATE_RESOURCE_GENERAL_REFERENCE_SEEDS = [
  "https://www.vote.org/",
] as const;

export const STATE_RESOURCE_MAIL_REFERENCE_SEEDS = ["https://vote.gov/register"] as const;
export const STATE_RESOURCE_ONLINE_REGISTRATION_REFERENCE_SEEDS = ["https://vote.gov/register"] as const;

// Backward-compatible combined seed list used by current draft payload schema.
export const STATE_RESOURCE_SEED_SOURCES = [
  ...STATE_RESOURCE_POLLING_REFERENCE_SEEDS,
  ...STATE_RESOURCE_GENERAL_REFERENCE_SEEDS,
  ...STATE_RESOURCE_MAIL_REFERENCE_SEEDS,
  ...STATE_RESOURCE_ONLINE_REGISTRATION_REFERENCE_SEEDS,
] as const;

// Documentation/audit reference for deterministic FIPS -> abbreviation mapping.
export const STATE_ABBREVIATION_REFERENCE_URL =
  "https://pe.usps.com/text/pub28/28apb.htm";

export const ALLOW_OPEN_WEB_RESEARCH = true;

export const STAGING_DRAFT_STREAM = "staging:draft";
export const STAGING_PENDING_STREAM = "staging:pending";
export const STAGING_VALIDATED_STREAM = "staging:validated";
export const STAGING_REJECTED_STREAM = "staging:rejected";
export const STAGING_WRITTEN_STREAM = "staging:written";

export const STAGING_STATE_RESOURCES_VALIDATOR_GROUP = "state_resources_validator";
export const STAGING_STATE_RESOURCES_WRITER_GROUP = "state_resources_writer";
export const STAGING_STATE_RESOURCES_ENRICHER_GROUP = "state_resources_enricher";

export const STAGING_ITEM_TYPE_STATE_RESOURCES = "state_resources" as const;

export const EXPECTED_STATE_RESOURCE_STATE_COUNT = 51;
