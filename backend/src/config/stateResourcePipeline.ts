export const CENSUS_STATES_API_URL =
  "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*";

export const STATE_RESOURCE_POLLING_PLACES_REFERENCE_SEEDS = [
  "https://www.vote.org/polling-place-locator/",
  "https://www.nass.org/can-i-vote/find-your-polling-place",
  "https://www.usvotefoundation.org/find-my-polling-place",
] as const;
export const STATE_RESOURCE_POLLING_HOURS_REFERENCE_SEED =
  "https://www.ncsl.org/elections-and-campaigns/polling-places";
export const STATE_RESOURCE_ID_REQUIREMENTS_REFERENCE_SEED =
  "https://www.ncsl.org/elections-and-campaigns/voter-id";

export const STATE_RESOURCE_SAME_DAY_REGISTRATION_DEADLINE_REFERENCE_SEED =
  "https://www.ncsl.org/elections-and-campaigns/same-day-voter-registration";
export const STATE_RESOURCE_EARLY_VOTING_REFERENCE_SEED =
  "https://www.ncsl.org/elections-and-campaigns/early-in-person-voting";

export const STATE_RESOURCE_MAIL_REFERENCE_SEED = "https://vote.gov/register";
export const STATE_RESOURCE_ONLINE_REGISTRATION_REFERENCE_SEED = "https://vote.gov/register";
export const STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_REFERENCE_SEED = "https://vote.gov/register";

// Backward-compatible combined seed list used by current draft payload schema.
export const STATE_RESOURCE_SEED_SOURCES = [
  ...STATE_RESOURCE_POLLING_PLACES_REFERENCE_SEEDS,
  STATE_RESOURCE_POLLING_HOURS_REFERENCE_SEED,
  STATE_RESOURCE_ID_REQUIREMENTS_REFERENCE_SEED,
  STATE_RESOURCE_SAME_DAY_REGISTRATION_DEADLINE_REFERENCE_SEED,
  STATE_RESOURCE_EARLY_VOTING_REFERENCE_SEED,
  STATE_RESOURCE_MAIL_REFERENCE_SEED,
  STATE_RESOURCE_ONLINE_REGISTRATION_REFERENCE_SEED,
  STATE_RESOURCE_IN_PERSON_REGISTRATION_DEADLINE_REFERENCE_SEED,
] as const;

// Documentation/audit reference for deterministic FIPS -> abbreviation mapping.
export const STATE_ABBREVIATION_REFERENCE_URL =
  "https://pe.usps.com/text/pub28/28apb.htm";

export const ALLOW_OPEN_WEB_RESEARCH = true;

export const STAGING_DRAFT_STREAM = "staging:state_resources:draft";
export const STAGING_PENDING_STREAM = "staging:state_resources:pending";
export const STAGING_VALIDATED_STREAM = "staging:state_resources:validated";
export const STAGING_REJECTED_STREAM = "staging:state_resources:rejected";
export const STAGING_WRITTEN_STREAM = "staging:state_resources:written";

export const STAGING_STATE_RESOURCES_VALIDATOR_GROUP = "state_resources_validator";
export const STAGING_STATE_RESOURCES_WRITER_GROUP = "state_resources_writer";
export const STAGING_STATE_RESOURCES_ENRICHER_GROUP = "state_resources_enricher";

export const STAGING_ITEM_TYPE_STATE_RESOURCES = "state_resources" as const;

export const EXPECTED_STATE_RESOURCE_STATE_COUNT = 51;
