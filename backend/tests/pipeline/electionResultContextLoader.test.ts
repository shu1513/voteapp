import { describe, expect, it, vi } from "vitest";

import {
  chunkElectionResultContexts,
  loadElectionResultContexts,
} from "../../src/pipeline/electionResults/electionResultContextLoader.js";

describe("loadElectionResultContexts", () => {
  it("loads election, candidate roster, filing IDs, and ballot measure context", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "00000000-0000-0000-0000-000000000001",
            district_id: "district-1",
            district_name: "California",
            district_type: "statewide",
            state: "CA",
            race_type: "office",
            official_ballot_title: "Governor",
            election_date: "2026-11-03",
            election_stage: "general",
            is_partisan: true,
            discovery_contest_family: "non_judicial_office",
            sources: ["https://elections.ca.gov/races"],
          },
          {
            election_id: "00000000-0000-0000-0000-000000000002",
            district_id: "district-1",
            district_name: "California",
            district_type: "statewide",
            state: "CA",
            race_type: "ballot_measure",
            official_ballot_title: "Proposition 1",
            election_date: "2026-11-03",
            election_stage: null,
            is_partisan: false,
            discovery_contest_family: "ballot_measure",
            sources: ["https://elections.ca.gov/measures"],
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "00000000-0000-0000-0000-000000000001",
            candidate_election_id: "ce-1",
            candidate_id: "candidate-1",
            display_name: "Jane Smith",
            party: "Democrat",
            is_incumbent: true,
            status: "declared",
            fec_ids: ["H6CA00001"],
            state_filing_ids: ["CA-123"],
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "00000000-0000-0000-0000-000000000002",
            ballot_measure_id: "bm-1",
            official_ballot_title: "Proposition 1",
            summary: "Measure summary",
            what_yes_means: "Yes means yes outcome",
            what_no_means: "No means no outcome",
            result: null,
            source_url: ["https://elections.ca.gov/prop1"],
            official_measure_url: "https://elections.ca.gov/prop1.pdf",
          },
        ],
      });

    const contexts = await loadElectionResultContexts(
      { query },
      [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
      ]
    );

    const candidateSql = query.mock.calls[1]?.[0] as string;
    expect(candidateSql).toContain(
      "ORDER BY ce.election_id, lower(COALESCE(NULLIF(trim(c.display_name), ''), trim(c.first_name || ' ' || c.last_name))), ce.id"
    );

    expect(contexts).toHaveLength(2);
    expect(contexts[0]).toMatchObject({
      electionId: "00000000-0000-0000-0000-000000000001",
      raceType: "office",
      officialBallotTitle: "Governor",
      sourceUrls: ["https://elections.ca.gov/races"],
      district: {
        name: "California",
        districtType: "statewide",
        state: "CA",
      },
      candidates: [
        {
          candidateElectionId: "ce-1",
          candidateId: "candidate-1",
          displayName: "Jane Smith",
          party: "Democrat",
          isIncumbent: true,
          status: "declared",
          fecIds: ["H6CA00001"],
          stateFilingIds: ["CA-123"],
        },
      ],
      ballotMeasure: null,
    });
    expect(contexts[1]?.ballotMeasure).toMatchObject({
      ballotMeasureId: "bm-1",
      summary: "Measure summary",
      sourceUrls: ["https://elections.ca.gov/prop1"],
      officialMeasureUrl: "https://elections.ca.gov/prop1.pdf",
    });
  });

  it("deduplicates input IDs and preserves requested order for found elections", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: "00000000-0000-0000-0000-000000000002",
            district_id: "district-1",
            district_name: "California",
            district_type: "statewide",
            state: "CA",
            race_type: "office",
            official_ballot_title: "Governor",
            election_date: "2026-11-03",
            election_stage: null,
            is_partisan: null,
            discovery_contest_family: null,
            sources: [],
          },
          {
            election_id: "00000000-0000-0000-0000-000000000001",
            district_id: "district-1",
            district_name: "California",
            district_type: "statewide",
            state: "CA",
            race_type: "office",
            official_ballot_title: "Attorney General",
            election_date: "2026-11-03",
            election_stage: null,
            is_partisan: null,
            discovery_contest_family: null,
            sources: [],
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });

    const contexts = await loadElectionResultContexts(
      { query },
      [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
      ]
    );

    expect(contexts.map((context) => context.electionId)).toEqual([
      "00000000-0000-0000-0000-000000000001",
      "00000000-0000-0000-0000-000000000002",
    ]);
  });
});

describe("chunkElectionResultContexts", () => {
  it("chunks contexts by requested size", () => {
    const contexts = Array.from({ length: 5 }, (_, index) => ({ electionId: `e-${index}` })) as never;

    const chunks = chunkElectionResultContexts(contexts, 2);

    expect(chunks.map((chunk) => chunk.length)).toEqual([2, 2, 1]);
  });

  it("rejects invalid chunk sizes", () => {
    expect(() => chunkElectionResultContexts([], 0)).toThrow("chunkSize must be a positive integer");
  });
});
