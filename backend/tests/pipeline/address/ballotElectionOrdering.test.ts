import { describe, expect, it, vi } from "vitest";

import { applyBallotElectionOrdering } from "../../../src/pipeline/address/ballotElectionOrdering.js";
import type { BallotLookupElectionSummary, BallotSummaryResult } from "../../../src/pipeline/address/ballotLookup.js";

const districtId = "11111111-1111-4111-8111-111111111111";
const candidateId = "44444444-4444-4444-8444-444444444444";
const userId = "99999999-9999-4999-8999-999999999999";

const electionA = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa";
const electionB = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb";
const electionC = "cccccccc-3333-4333-8333-cccccccccccc";

type FakeElection = {
  id: string;
  election_date?: string;
  vote_power_score?: number | null;
  population?: number | null;
};

function makeSummary(elections: FakeElection[]): BallotSummaryResult {
  return {
    district_ids: [districtId],
    districts: [],
    elections: elections.map(
      (e): BallotLookupElectionSummary => ({
        id: e.id,
        district_id: districtId,
        district: {
          id: districtId,
          district_type: "county",
          geoid_compact: "06037",
          name: "Los Angeles County",
          state: "CA",
          state_fips: "06",
          representation_power_score: 50,
          population: e.population ?? null,
        },
        race_type: "office",
        official_ballot_title: "Office",
        election_date: e.election_date ?? "2026-11-03",
        election_stage: "general",
        is_partisan: false,
        discovery_contest_family: "non_judicial_office",
        sources: [],
        candidate_count: 2,
        ballot_measure_id: null,
        has_results: false,
        current_result_outcome: null,
        office: null,
        research_areas: [],
        historical_competitiveness: null,
        vote_power: {
          score: e.vote_power_score ?? 50,
          label: "medium",
          confidence: "medium",
          representation_level: "medium",
          decisiveness_level: "medium",
          factors: [],
        },
      })
    ),
  };
}

function makeFollowsQuery(rows: { election_id: string; candidate_id: string; display_name: string }[]) {
  return vi.fn().mockResolvedValue({ rows });
}

describe("applyBallotElectionOrdering", () => {
  it("orders by vote_power score descending by default", async () => {
    const query = makeFollowsQuery([]);
    const result = await applyBallotElectionOrdering(
      { query },
      makeSummary([
        { id: electionA, vote_power_score: 20 },
        { id: electionB, vote_power_score: 95 },
      ])
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionB, electionA]);
    expect(result.elections.every((e) => e.followed_candidates.length === 0)).toBe(true);
  });

  it("orders by earliest date when sort=soonest, ignoring vote power", async () => {
    const result = await applyBallotElectionOrdering(
      { query: makeFollowsQuery([]) },
      makeSummary([
        { id: electionA, election_date: "2026-06-02", vote_power_score: 10 },
        { id: electionB, election_date: "2026-11-03", vote_power_score: 95 },
      ]),
      { sort: "soonest" }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionA, electionB]);
  });

  it("orders by district population descending when sort=district_size, unknown populations last", async () => {
    const result = await applyBallotElectionOrdering(
      { query: makeFollowsQuery([]) },
      makeSummary([
        { id: electionA, vote_power_score: 95, population: 250_000 },
        { id: electionB, vote_power_score: 10, population: 9_800_000 },
        { id: electionC, vote_power_score: 95, population: null },
      ]),
      { sort: "district_size" }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionB, electionA, electionC]);
  });

  it("orders by district population ascending when sort=district_size_smallest, unknown populations still last", async () => {
    const result = await applyBallotElectionOrdering(
      { query: makeFollowsQuery([]) },
      makeSummary([
        { id: electionA, population: 250_000 },
        { id: electionB, population: 9_800_000 },
        { id: electionC, population: null },
      ]),
      { sort: "district_size_smallest" }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionA, electionB, electionC]);
  });

  it("groups followed elections first by default for authenticated calls and annotates them", async () => {
    const query = makeFollowsQuery([{ election_id: electionA, candidate_id: candidateId, display_name: "Jane Doe" }]);
    const result = await applyBallotElectionOrdering(
      { query },
      makeSummary([
        { id: electionA, vote_power_score: 10 },
        { id: electionB, vote_power_score: 95 },
      ]),
      { userId }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionA, electionB]);
    expect(result.elections[0]?.followed_candidates).toEqual([
      { candidate_id: candidateId, display_name: "Jane Doe" },
    ]);
    expect(result.elections[1]?.followed_candidates).toEqual([]);
  });

  it("does not group followed elections first when followedFirst is explicitly off, but still annotates them", async () => {
    const query = makeFollowsQuery([{ election_id: electionA, candidate_id: candidateId, display_name: "Jane Doe" }]);
    const result = await applyBallotElectionOrdering(
      { query },
      makeSummary([
        { id: electionA, vote_power_score: 10 },
        { id: electionB, vote_power_score: 95 },
      ]),
      { userId, followedFirst: false }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionB, electionA]);
    expect(result.elections.find((e) => e.id === electionA)?.followed_candidates).toEqual([
      { candidate_id: candidateId, display_name: "Jane Doe" },
    ]);
  });

  it("skips the follow query entirely for anonymous calls", async () => {
    const query = makeFollowsQuery([{ election_id: electionA, candidate_id: candidateId, display_name: "Jane Doe" }]);
    const result = await applyBallotElectionOrdering({ query }, makeSummary([{ id: electionA }]));

    expect(query).not.toHaveBeenCalled();
    expect(result.elections[0]?.followed_candidates).toEqual([]);
  });

  it("filters followed candidates to live, unmerged, still-competing ballot links in SQL", async () => {
    const query = makeFollowsQuery([]);
    await applyBallotElectionOrdering({ query }, makeSummary([{ id: electionA }]), { userId });

    expect(query).toHaveBeenCalledTimes(1);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("ce.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("c.merged_into_candidate_id IS NULL");
    expect(sql).toContain("concat_ws(' ', c.first_name, c.last_name)");
  });
});
