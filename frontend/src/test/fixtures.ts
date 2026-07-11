// Minimal typed payload builders for page tests. Only the fields the pages
// actually render get non-trivial values; everything else is a quiet default.

import type {
  BallotSummary,
  CandidateDetail,
  CandidateFollow,
  ElectionDetail,
  ElectionSummary,
  FinanceSummary,
  VotePower,
} from "@voteapp/api-client";

export const ME_VERIFIED = {
  user: { email: "voter@example.com", first_name: "Sam", email_verified: true },
};

export const ME_UNVERIFIED = {
  user: { email: "voter@example.com", first_name: "Sam", email_verified: false },
};

export const VOTE_POWER: VotePower = {
  score: 42,
  label: "high",
  confidence: "medium",
  representation_level: "medium",
  decisiveness_level: "high",
};

const DISTRICT = { id: "d-1", district_type: "state", name: "Alaska", state: "AK" };

export function electionSummary(overrides: Partial<ElectionSummary> = {}): ElectionSummary {
  return {
    id: "e-1",
    district_id: DISTRICT.id,
    district: DISTRICT,
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    election_stage: null,
    is_partisan: true,
    candidate_count: 2,
    ballot_measure_id: null,
    has_results: false,
    current_result_outcome: null,
    office: null,
    research_areas: [],
    historical_competitiveness: null,
    vote_power: VOTE_POWER,
    ...overrides,
  };
}

export function ballotSummary(elections: ElectionSummary[]): BallotSummary {
  return { district_ids: [DISTRICT.id], districts: [DISTRICT], elections };
}

export function electionDetail(overrides: Partial<ElectionDetail> = {}): ElectionDetail {
  return {
    id: "e-1",
    district_id: DISTRICT.id,
    district: DISTRICT,
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    election_stage: null,
    is_partisan: true,
    sources: ["https://elections.example.gov"],
    candidates: [
      {
        candidate_id: "c-1",
        display_name: "Jordan Voter",
        party: "Independent",
        is_incumbent: false,
        status: "active",
        summary: "A candidate summary.",
        finance_summary: null,
        records: [],
      },
      {
        candidate_id: "c-2",
        display_name: "Riley Runner",
        party: "Independent",
        is_incumbent: true,
        status: "active",
        summary: null,
        finance_summary: null,
        records: [],
      },
    ],
    ballot_measure: null,
    results: [],
    historical_competitiveness: null,
    vote_power: VOTE_POWER,
    ...overrides,
  };
}

export function candidateDetail(
  overrides: Partial<CandidateDetail["candidate"]> = {}
): CandidateDetail {
  return {
    candidate: {
      candidate_id: "c-1",
      display_name: "Jordan Voter",
      party: "Independent",
      state: "AK",
      current_office: null,
      summary: "A candidate summary.",
      twitter_handle: null,
      linkedin_url: null,
      official_website_url: null,
      profile_sources: ["https://example.gov/profile"],
      last_researched: "2026-06-01T00:00:00.000Z",
      records: [
        {
          id: "r-1",
          description: "Voted for the clean water act.",
          source_url: "https://example.gov/record",
          event_date: "2026-05-01",
          created_at: "2026-05-02T00:00:00.000Z",
          research_area_tags: [
            { research_area_id: "a-env", slug: "environment", name: "Environment", stance: "for" },
          ],
        },
      ],
      elections: [],
      is_following: false,
      ...overrides,
    },
  };
}

/** A summary with every money field null and every list empty — the shape a
 * state adapter emits when a committee exists but nothing is disclosed yet. */
export function emptyFinanceSummary(): FinanceSummary {
  return {
    source: "FEC",
    cycle: 2026,
    last_synced_at: "2026-07-01T00:00:00.000Z",
    direct_campaign: {
      total_raised: null,
      total_spent: null,
      cash_on_hand: null,
      debts_owed: null,
      top_occupations: [],
      top_industries: [],
    },
    outside_spending: {
      support_total: null,
      oppose_total: null,
      top_supporting_groups: [],
      top_opposing_groups: [],
      top_supporting_industries: [],
      top_opposing_industries: [],
    },
  };
}

/** A fully populated summary covering every section the card renders. */
export function financeSummary(): FinanceSummary {
  return {
    source: "FEC",
    cycle: 2026,
    last_synced_at: "2026-07-01T00:00:00.000Z",
    direct_campaign: {
      total_raised: 120000,
      total_spent: 80000,
      cash_on_hand: 40000,
      debts_owed: 0,
      top_occupations: [
        { category_name: "Retired", amount: 30000, contributor_count: 40, source_url: null },
        { category_name: "Attorney", amount: 20000, contributor_count: 12, source_url: null },
      ],
      top_industries: [
        {
          category_name: "oil_gas_energy",
          amount: 25000,
          contributor_count: 9,
          source_url: "https://www.fec.gov/data/candidate/H0AK00001/",
        },
      ],
    },
    outside_spending: {
      support_total: 50000,
      oppose_total: 20000,
      top_supporting_groups: [
        { committee_id: "pac-1", committee_name: "Growth PAC", support_oppose: "support", amount: 50000, source_url: null },
      ],
      top_opposing_groups: [
        { committee_id: "pac-2", committee_name: "Stop Them PAC", support_oppose: "oppose", amount: 20000, source_url: null },
      ],
      top_supporting_industries: [
        { category_name: "technology", amount: 35000, contributor_count: null, source_url: null },
      ],
      top_opposing_industries: [
        { category_name: "labor_unions", amount: 15000, contributor_count: null, source_url: null },
      ],
    },
  };
}

export function candidateFollow(overrides: Partial<CandidateFollow> = {}): CandidateFollow {
  return {
    candidate_id: "c-1",
    display_name: "Jordan Voter",
    party: "Independent",
    state: "AK",
    current_office: null,
    latest_record: null,
    active_election: null,
    notify_elections: true,
    notify_updates: true,
    created_at: "2026-06-01T00:00:00.000Z",
    ...overrides,
  };
}
