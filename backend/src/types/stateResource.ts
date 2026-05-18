export type StateResourceItemType = "state_resources";

export type SourceCitation = string;

export type StateResourceSources = {
  polling_place_url: SourceCitation[];
  mail_voting_available: SourceCitation[];
  mail_ballot_request_deadline_rule: SourceCitation[];
  mail_ballot_return_deadline_rule: SourceCitation[];
  mail_ballot_return_deadline_type: SourceCitation[];
  early_voting_available: SourceCitation[];
  early_voting_start_date_rule: SourceCitation[];
  early_voting_end_date_rule: SourceCitation[];
  polling_hours: SourceCitation[];
  id_requirements: SourceCitation[];
  same_day_registration_available: SourceCitation[];
  online_registration_available: SourceCitation[];
  online_registration_deadline_rule: SourceCitation[];
  in_person_registration_deadline_rule: SourceCitation[];
};

export type StateResourcePayload = {
  state_fips: string;
  state_abbreviation: string;
  state_name: string;
  polling_place_url: string;
  voter_registration_url: string;
  mail_voting_available: boolean;
  mail_ballot_request_deadline_rule: string | null;
  mail_ballot_return_deadline_rule: string | null;
  mail_ballot_return_deadline_type: "postmarked_by" | "received_by" | null;
  early_voting_available: boolean;
  early_voting_start_date_rule: string | null;
  early_voting_end_date_rule: string | null;
  polling_hours: string;
  id_requirements: string;
  same_day_registration_available: boolean;
  online_registration_available: boolean;
  online_registration_deadline_rule: string | null;
  in_person_registration_deadline_rule: string;
  sources: StateResourceSources;
};

export type StateResourceDraftPayload = {
  state_fips: string;
  state_abbreviation: string;
  state_name: string;
  population_estimate: number | null;
  census_source_url: string;
  state_abbreviation_reference_url: string;
  seed_sources: readonly string[];
};
