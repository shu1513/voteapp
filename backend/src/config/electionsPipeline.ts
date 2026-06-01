export const STAGING_DRAFT_STREAM = "staging:elections:draft";
export const STAGING_PENDING_STREAM = "staging:elections:pending";
export const STAGING_VALIDATED_STREAM = "staging:elections:validated";
export const STAGING_REJECTED_STREAM = "staging:elections:rejected";
export const STAGING_WRITTEN_STREAM = "staging:elections:written";
export const STAGING_BALLOT_MEASURE_DRAFT_STREAM = "staging:ballot-measures:draft";
export const STAGING_BALLOT_MEASURE_REJECTED_STREAM = "staging:ballot-measures:rejected";
export const STAGING_CANDIDATE_ROSTER_DRAFT_STREAM = "staging:candidates:roster:draft";
export const STAGING_CANDIDATE_ROSTER_REJECTED_STREAM = "staging:candidates:roster:rejected";
export const STAGING_CANDIDATE_PROFILE_DRAFT_STREAM = "staging:candidates:profile:draft";
export const STAGING_CANDIDATE_PROFILE_REJECTED_STREAM = "staging:candidates:profile:rejected";
export const STAGING_CANDIDATE_RECORD_DRAFT_STREAM = "staging:candidates:record:draft";
export const STAGING_CANDIDATE_RECORD_REJECTED_STREAM = "staging:candidates:record:rejected";

export const STAGING_ELECTIONS_ENRICHER_GROUP = "elections_enricher";
export const STAGING_ELECTIONS_VALIDATOR_GROUP = "elections_validator";
export const STAGING_ELECTIONS_WRITER_GROUP = "elections_writer";
export const STAGING_BALLOT_MEASURE_ENRICHER_GROUP = "ballot_measures_enricher";
export const STAGING_CANDIDATE_ROSTER_ENRICHER_GROUP = "candidate_roster_enricher";
export const STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP = "candidate_profile_enricher";
export const STAGING_CANDIDATE_RECORD_ENRICHER_GROUP = "candidate_record_enricher";

export const STAGING_ITEM_TYPE_ELECTION = "election" as const;
export const STAGING_ITEM_TYPE_BALLOT_MEASURE = "ballot_measure" as const;
export const STAGING_ITEM_TYPE_CANDIDATE_ROSTER = "candidate_roster" as const;
export const STAGING_ITEM_TYPE_CANDIDATE_PROFILE = "candidate_profile" as const;
export const STAGING_ITEM_TYPE_CANDIDATE_RECORD = "candidate_record" as const;
