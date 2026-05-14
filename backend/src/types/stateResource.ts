export type StateResourceItemType = "state_resources";

export type SourceCitation = string;

export type StateResourceSources = {
  polling_place_url: SourceCitation[];
  mail_voting_available: SourceCitation[];
  mail_ballot_request_deadline_rule: SourceCitation[];
  mail_ballot_return_deadline_rule: SourceCitation[];
  mail_ballot_return_deadline_type: SourceCitation[];
  polling_hours: SourceCitation[];
  id_requirements: SourceCitation[];
  same_day_registration_available: SourceCitation[];
  online_registration_available: SourceCitation[];
  online_registration_deadline_rule: SourceCitation[];
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
  polling_hours: string;
  id_requirements: string;
  same_day_registration_available: boolean;
  online_registration_available: boolean;
  online_registration_deadline_rule: string | null;
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
  allow_open_web_research: boolean;
};
