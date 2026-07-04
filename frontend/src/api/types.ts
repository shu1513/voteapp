// Hand-written mirrors of the backend's JSON payloads (snake_case on the
// wire, kept snake_case here so the mapping is greppable). Only the fields
// Phase 1 renders plus stable identifiers; extend as later phases need more.

export type ResolvedDistrict = {
  id: string;
  district_type: string;
  geoid_compact: string;
  name: string;
  state: string;
  population: number;
  representation_power_score: number | null;
};

export type AddressResolution = {
  matched_address: string;
  districts: ResolvedDistrict[];
};

export type ResearchAreaSummary = {
  id: string;
  slug: string;
  name: string;
  description: string | null;
};

export type OfficeSummary = {
  id: string;
  scope: string;
  canonical_name: string;
  summary: string;
};

export type VotePower = {
  score: number | null;
  label: "very_low" | "low" | "medium" | "high" | "very_high" | "unknown";
  confidence: string;
  representation_level: string;
  decisiveness_level: string;
};

export type HistoricalCompetitiveness = {
  display_label: string;
  display_description: string;
  source: string;
  source_url: string | null;
  election_year: number;
  margin_percent: number;
  stale_after_redistricting: boolean;
};

export type BallotDistrict = {
  id: string;
  district_type: string;
  name: string;
  state: string;
};

export type ElectionSummary = {
  id: string;
  district_id: string;
  district: BallotDistrict;
  race_type: string;
  official_ballot_title: string;
  election_date: string;
  election_stage: string | null;
  is_partisan: boolean | null;
  candidate_count: number;
  ballot_measure_id: string | null;
  has_results: boolean;
  current_result_outcome: string | null;
  office: OfficeSummary | null;
  research_areas: ResearchAreaSummary[];
  historical_competitiveness: HistoricalCompetitiveness | null;
  vote_power: VotePower;
};

export type BallotSummary = {
  district_ids: string[];
  districts: BallotDistrict[];
  elections: ElectionSummary[];
};

export type FinanceBreakdown = {
  total_amount: number | null;
  [key: string]: unknown;
};

export type CandidateFinance = {
  total_raised: number | null;
  total_spent: number | null;
  cash_on_hand: number | null;
  source: string | null;
  source_url: string | null;
  last_synced: string | null;
  [key: string]: unknown;
};

export type ElectionCandidate = {
  candidate_id: string;
  display_name: string;
  party: string;
  is_incumbent: boolean;
  status: string;
  summary: string | null;
  finance: CandidateFinance | null;
  [key: string]: unknown;
};

export type BallotMeasureResult = {
  outcome: string | null;
  [key: string]: unknown;
};

export type BallotMeasure = {
  id: string;
  official_ballot_title: string;
  summary: string | null;
  what_yes_means: string;
  what_no_means: string;
  result: "passed" | "failed" | null;
  source_urls: string[];
  official_measure_url: string | null;
  research_area_tags: { research_area_id: string; slug: string; name: string; stance: string | null }[];
};

export type ElectionResult = {
  [key: string]: unknown;
};

export type ElectionDetail = {
  id: string;
  district_id: string;
  district: BallotDistrict;
  race_type: string;
  official_ballot_title: string;
  election_date: string;
  election_stage: string | null;
  is_partisan: boolean | null;
  sources: string[];
  candidates: ElectionCandidate[];
  ballot_measure: BallotMeasure | null;
  results: ElectionResult[];
  historical_competitiveness: HistoricalCompetitiveness | null;
  vote_power: VotePower;
};

export type CandidateRecordTag = {
  research_area_id: string;
  slug: string;
  name: string;
  stance: "for" | "against" | null;
};

export type CandidateRecord = {
  id: string;
  description: string;
  source_url: string;
  event_date: string;
  created_at: string;
  research_area_tags: CandidateRecordTag[];
};

export type CandidateElection = {
  candidate_election_id: string;
  election_id: string;
  district: { id: string; name: string; district_type: string; state: string };
  race_type: string;
  official_ballot_title: string;
  election_date: string;
  is_incumbent: boolean;
  status: string;
  office_canonical_name: string | null;
};

export type CandidateDetail = {
  candidate: {
    candidate_id: string;
    display_name: string;
    party: string;
    state: string;
    current_office: string | null;
    summary: string | null;
    twitter_handle: string | null;
    linkedin_url: string | null;
    official_website_url: string | null;
    profile_sources: string[];
    last_researched: string | null;
    records: CandidateRecord[];
    elections: CandidateElection[];
    is_following: boolean;
  };
};
