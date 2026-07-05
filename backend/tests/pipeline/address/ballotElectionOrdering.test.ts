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
  research_area_ids?: string[];
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
        research_areas: (e.research_area_ids ?? []).map((areaId) => ({
          id: areaId,
          slug: areaId,
          name: areaId,
          description: null,
        })),
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

// Routes the decorator's two queries (candidate follows, research-area
// preference weights) by their distinguishing SQL fragments.
function makeOrderingQuery(fixtures: {
  follows?: { election_id: string; candidate_id: string; display_name: string }[];
  areaPreferences?: { research_area_id: string; rank: number | null }[];
}) {
  return vi.fn(async (sql: string) => {
    if (sql.includes("user_research_area_preferences")) {
      return { rows: fixtures.areaPreferences ?? [] };
    }
    if (sql.includes("user_candidate_follows")) {
      return { rows: fixtures.follows ?? [] };
    }
    throw new Error(`Unexpected SQL in test: ${sql.slice(0, 80)}`);
  });
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

  it("orders by summed saved-area weights when sort=my_areas, non-matching elections last", async () => {
    const areaTop = "aaaaaaaa-0000-4000-8000-000000000001";
    const areaMid = "aaaaaaaa-0000-4000-8000-000000000002";
    const areaLow = "aaaaaaaa-0000-4000-8000-000000000003";
    const query = makeOrderingQuery({
      areaPreferences: [
        { research_area_id: areaTop, rank: 1 }, // weight 7
        { research_area_id: areaMid, rank: 3 }, // weight 5
        { research_area_id: areaLow, rank: 4 }, // weight 4
      ],
    });
    const result = await applyBallotElectionOrdering(
      { query },
      makeSummary([
        { id: electionA, research_area_ids: [areaTop], vote_power_score: 99 }, // score 7
        { id: electionB, research_area_ids: [areaMid, areaLow], vote_power_score: 1 }, // score 9
        { id: electionC, research_area_ids: [], vote_power_score: 100 }, // no match
      ]),
      { userId, sort: "my_areas" }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionB, electionA, electionC]);
  });

  it("breaks equal my_areas sums by the best matched rank", async () => {
    const areaRankTwo = "aaaaaaaa-0000-4000-8000-000000000004";
    const areaRankThree = "aaaaaaaa-0000-4000-8000-000000000005";
    const areaRankSeven = "aaaaaaaa-0000-4000-8000-000000000006";
    const query = makeOrderingQuery({
      areaPreferences: [
        { research_area_id: areaRankTwo, rank: 2 }, // weight 6
        { research_area_id: areaRankThree, rank: 3 }, // weight 5
        { research_area_id: areaRankSeven, rank: 7 }, // weight 1
      ],
    });
    const result = await applyBallotElectionOrdering(
      { query },
      makeSummary([
        // Both sum to 6; the election touching rank 2 wins over rank 3 + rank 7.
        { id: electionA, research_area_ids: [areaRankThree, areaRankSeven], vote_power_score: 99 },
        { id: electionB, research_area_ids: [areaRankTwo], vote_power_score: 1 },
      ]),
      { userId, sort: "my_areas" }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionB, electionA]);
  });

  it("degrades my_areas to vote_power for anonymous calls without querying weights", async () => {
    const query = makeOrderingQuery({});
    const result = await applyBallotElectionOrdering(
      { query },
      makeSummary([
        { id: electionA, vote_power_score: 10 },
        { id: electionB, vote_power_score: 90 },
      ]),
      { sort: "my_areas" }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionB, electionA]);
    expect(query).not.toHaveBeenCalled();
  });

  it("degrades my_areas to vote_power when the user saved no research areas", async () => {
    const query = makeOrderingQuery({ areaPreferences: [] });
    const result = await applyBallotElectionOrdering(
      { query },
      makeSummary([
        { id: electionA, vote_power_score: 10 },
        { id: electionB, vote_power_score: 90 },
      ]),
      { userId, sort: "my_areas" }
    );

    expect(result.elections.map((e) => e.id)).toEqual([electionB, electionA]);
  });

  it("keeps followed elections grouped first under my_areas", async () => {
    const areaTop = "aaaaaaaa-0000-4000-8000-000000000007";
    const query = makeOrderingQuery({
      follows: [{ election_id: electionC, candidate_id: candidateId, display_name: "Jane Doe" }],
      areaPreferences: [{ research_area_id: areaTop, rank: 1 }],
    });
    const result = await applyBallotElectionOrdering(
      { query },
      makeSummary([
        { id: electionA, research_area_ids: [areaTop] },
        { id: electionC, research_area_ids: [] },
      ]),
      { userId, sort: "my_areas" }
    );

    // electionC matches nothing but contains a followed candidate.
    expect(result.elections.map((e) => e.id)).toEqual([electionC, electionA]);
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
