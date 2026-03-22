export const CENSUS_STATES_API_URL =
  "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*";

export const STATE_RESOURCE_SEED_SOURCES = [
  "https://www.vote.org/polling-place-locator/",
  "https://www.nass.org/can-i-vote/find-your-polling-place",
  "https://www.usvotefoundation.org/find-my-polling-place",
] as const;

export const ALLOW_OPEN_WEB_RESEARCH = true;

export const STAGING_PENDING_STREAM = "staging:pending";

export const STAGING_ITEM_TYPE_STATE_RESOURCES = "state_resources" as const;

export const EXPECTED_STATE_RESOURCE_STATE_COUNT = 51;
