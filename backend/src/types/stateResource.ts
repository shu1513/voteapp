export type StateResourceItemType = "state_resources";

export type SourceCitation = {
  source_url: string;
  source_name: string;
};

export type StateResourceSources = {
  polling_place_url: SourceCitation[];
  voter_registration_url: SourceCitation[];
  vote_by_mail_info: SourceCitation[];
  polling_hours: SourceCitation[];
  id_requirements: SourceCitation[];
};

export type StateResourcePayload = {
  state_fips: string;
  state_abbreviation: string;
  state_name: string;
  polling_place_url: string;
  voter_registration_url: string;
  vote_by_mail_info: string;
  polling_hours: string;
  id_requirements: string;
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
