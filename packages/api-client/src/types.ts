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

export type AddressSuggestion = {
  place_id: string;
  description: string;
  main_text: string;
  secondary_text: string;
};

export type AddressAutocompleteResponse = {
  suggestions: AddressSuggestion[];
};

export type AddressRetrieveResponse = {
  address: string;
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
  /** Present on ordered results; non-empty when the viewer follows a candidate in this election. */
  followed_candidates?: { candidate_id: string; display_name: string }[];
};

export type BallotSummary = {
  district_ids: string[];
  districts: BallotDistrict[];
  elections: ElectionSummary[];
};

// Mirrors BallotLookupFinanceBreakdown (backend ballotLookupFinanceShared.ts).
// Industry category_name values arrive as slugs (oil_gas_energy) — display
// them through formatFinanceCategory; occupation names arrive as free text.
export type FinanceBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number | null;
  source_url: string | null;
};

// Mirrors BallotLookupFinanceOutsideGroup (backend ballotLookupFinanceShared.ts).
export type FinanceOutsideGroup = {
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: number;
  source_url: string | null;
};

// Mirrors BallotLookupFinanceSummary (backend ballotLookupFinanceShared.ts):
// the money lives under direct_campaign, not at the top level. Deliberately
// partial — only what the UI renders; backing_summary stays backend-only.
// null money values mean "not reported"; 0 is a real disclosed amount.
export type FinanceSummary = {
  source: string;
  cycle: number;
  last_synced_at: string;
  direct_campaign: {
    total_raised: number | null;
    total_spent: number | null;
    cash_on_hand: number | null;
    debts_owed: number | null;
    public_funds_received?: number | null;
    top_occupations: FinanceBreakdown[];
    top_employers?: FinanceBreakdown[];
    top_industries: FinanceBreakdown[];
    contribution_size_buckets?: FinanceBreakdown[];
  };
  outside_spending: {
    support_total: number | null;
    oppose_total: number | null;
    top_supporting_groups: FinanceOutsideGroup[];
    top_opposing_groups: FinanceOutsideGroup[];
    top_supporting_industries: FinanceBreakdown[];
    top_opposing_industries: FinanceBreakdown[];
  };
};

export type ElectionCandidate = {
  candidate_id: string;
  display_name: string;
  party: string;
  is_incumbent: boolean;
  status: string;
  summary: string | null;
  finance_summary: FinanceSummary | null;
  /** Full record history with research-area stance tags; drives the stance chips. */
  records: CandidateRecord[];
  [key: string]: unknown;
};

// Mirrors BallotLookupBallotMeasureResult (backend ballotLookup.ts): one row
// per results pass (election_night, certified) for the measure.
export type BallotMeasureResult = {
  id: string;
  pass_type: string;
  result_status: string;
  outcome: string;
  source_url: string;
  source_type: string;
  retrieved_at: string;
};

export type BallotMeasure = {
  id: string;
  official_ballot_title: string;
  summary: string | null;
  what_yes_means: string;
  what_no_means: string;
  /** Canonical outcome, set only from a certified official verified source. */
  result: "passed" | "failed" | null;
  source_urls: string[];
  official_measure_url: string | null;
  research_area_tags: { research_area_id: string; slug: string; name: string; stance: string | null }[];
  results: BallotMeasureResult[];
};

export type ElectionResultWinner = {
  candidate_id?: string;
  candidate_name?: string;
  party?: string;
};

export type ElectionResult = {
  id: string;
  pass_type: string;
  result_status: string;
  outcome: string;
  winners: ElectionResultWinner[];
  source_url: string;
  retrieved_at: string;
};

// Mirrors BALLOT_SUMMARY_SORTS (backend ballotElectionOrdering.ts). my_areas
// is also the backend's default for users who saved research areas but never
// chose a sort, so this list must always be able to represent it.
export const BALLOT_SORTS = [
  { value: "my_areas", label: "My issues" },
  { value: "vote_power", label: "Vote power" },
  { value: "soonest", label: "Soonest first" },
  { value: "district_size", label: "Biggest districts" },
  { value: "district_size_smallest", label: "Smallest districts" },
] as const;

export type BallotSort = (typeof BALLOT_SORTS)[number]["value"];

// Sorts the anonymous /api/ballot page may offer. my_areas needs a signed-in
// user with saved research areas — the public endpoint has no user, so the
// backend would silently degrade it to vote_power while the UI claimed an
// issue-based order.
export const PUBLIC_BALLOT_SORTS = BALLOT_SORTS.filter((option) => option.value !== "my_areas");

export const BALLOT_SORT_DESCRIPTIONS: Record<BallotSort, string> = {
  my_areas: "ordered by how much each race affects the issues you care about.",
  vote_power: "ordered by where your vote carries the most weight.",
  soonest: "ordered by election date, soonest first.",
  district_size: "ordered by district population, biggest first.",
  district_size_smallest: "ordered by district population, smallest first.",
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

export type CandidateFollow = {
  candidate_id: string;
  display_name: string;
  party: string;
  state: string;
  current_office: string | null;
  latest_record: { description: string; event_date: string } | null;
  active_election: { election_id: string; official_ballot_title: string; election_date: string } | null;
  notify_elections: boolean;
  notify_updates: boolean;
  created_at: string;
};

export type CandidateFollowsResult = {
  follows: CandidateFollow[];
};

export type CandidateFollowUpdate = {
  candidate_id: string;
  following: boolean;
  notify_elections?: boolean;
  notify_updates?: boolean;
};

export type BallotPreferences = {
  sort: BallotSort;
  followed_first: boolean;
};

export type EmailPreferences = {
  email_digest: boolean;
  email_election_reminders: boolean;
  email_new_election_alerts: boolean;
  email_issue_updates: boolean;
};

export type ResearchAreaCatalog = {
  research_areas: { id: string; slug: string; name: string; description: string | null }[];
};

export type ResearchAreaPreference = {
  research_area_id: string;
  slug: string;
  name: string;
  description: string | null;
  rank: number | null;
};

export type ResearchAreaPreferencesResult = {
  preferences: ResearchAreaPreference[];
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

export type ContentReportEntityType = "candidate" | "candidate_record" | "election" | "ballot_measure";

export type CreateContentReportResponse = {
  report: { id: string };
};
