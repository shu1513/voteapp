import { describe, expect, it, vi } from "vitest";

import { lookupBallotByDistrictIds } from "../../../src/pipeline/address/ballotLookup.js";

const districtId = "11111111-1111-4111-8111-111111111111";
const officeElectionId = "22222222-2222-4222-8222-222222222222";
const measureElectionId = "33333333-3333-4333-8333-333333333333";
const candidateId = "44444444-4444-4444-8444-444444444444";
const candidateElectionId = "55555555-5555-4555-8555-555555555555";
const candidateRecordId = "66666666-6666-4666-8666-666666666666";
const ballotMeasureId = "77777777-7777-4777-8777-777777777777";

describe("lookupBallotByDistrictIds", () => {
  it("returns an empty result without querying for empty district IDs", async () => {
    const db = { query: vi.fn() };

    await expect(lookupBallotByDistrictIds(db, [" "])).resolves.toEqual({
      district_ids: [],
      districts: [],
      elections: [],
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("loads elections with candidates, measures, results, records, and research tags", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: districtId,
            district_type: "county",
            geoid_compact: "06037",
            name: "Los Angeles County",
            state: "CA",
            state_fips: "06",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: officeElectionId,
            district_id: districtId,
            district_type: "county",
            geoid_compact: "06037",
            district_name: "Los Angeles County",
            state: "CA",
            state_fips: "06",
            race_type: "office",
            official_ballot_title: "Sheriff",
            election_date: "2026-06-02",
            election_stage: "primary",
            is_partisan: false,
            discovery_contest_family: "non_judicial_office",
            sources: ["https://example.test/elections"],
          },
          {
            election_id: measureElectionId,
            district_id: districtId,
            district_type: "county",
            geoid_compact: "06037",
            district_name: "Los Angeles County",
            state: "CA",
            state_fips: "06",
            race_type: "ballot_measure",
            official_ballot_title: "Measure H",
            election_date: "2026-06-02",
            election_stage: null,
            is_partisan: null,
            discovery_contest_family: "ballot_measure",
            sources: ["https://example.test/measure"],
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: officeElectionId,
            candidate_election_id: candidateElectionId,
            candidate_id: candidateId,
            display_name: "Pat Connolly",
            party: "Nonpartisan",
            is_incumbent: false,
            status: "declared",
            summary: "Trial attorney.",
            current_office: null,
            state: "CA",
            fec_ids: ["H1CA00001"],
            state_filing_ids: ["SF-1"],
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: measureElectionId,
            ballot_measure_id: ballotMeasureId,
            official_ballot_title: "Measure H",
            summary: "Funds hospitals.",
            what_yes_means: "Raises the tax.",
            what_no_means: "Keeps current tax rates.",
            result: null,
            source_url: ["https://example.test/measure-h"],
            official_measure_url: "https://example.test/measure-h/full-text",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: officeElectionId,
            id: "88888888-8888-4888-8888-888888888888",
            pass_type: "election_night",
            result_status: "unofficial",
            outcome: "won",
            winners: [{ candidate_election_id: candidateElectionId, candidate_name: "Pat Connolly" }],
            match_status: "matched",
            source_url: "https://results.example.test/office",
            source_type: "official",
            retrieved_at: "2026-06-03 04:10:00+00",
            raw_result: { source: "mock" },
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            ballot_measure_id: ballotMeasureId,
            id: "99999999-9999-4999-8999-999999999999",
            pass_type: "certified",
            result_status: "certified",
            outcome: "passed",
            source_url: "https://results.example.test/measure",
            source_type: "official",
            retrieved_at: "2026-07-10 04:10:00+00",
            raw_result: { source: "mock" },
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: candidateId,
            candidate_record_id: candidateRecordId,
            description: "Handled public corruption cases.",
            source_url: "https://example.test/record",
            event_date: "2025-05-01",
            created_at: "2026-06-04 00:00:00+00",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_record_id: candidateRecordId,
            research_area_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            slug: "anti_corruption",
            name: "Anti-Corruption",
            stance: "for",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            ballot_measure_id: ballotMeasureId,
            research_area_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            slug: "healthcare_affordability",
            name: "Healthcare Affordability",
            stance: "for",
          },
        ],
      });

    const result = await lookupBallotByDistrictIds({ query }, [districtId, districtId]);

    expect(result.district_ids).toEqual([districtId]);
    expect(result.districts).toHaveLength(1);
    expect(result.elections).toHaveLength(2);
    expect(result.elections[0]).toMatchObject({
      id: officeElectionId,
      race_type: "office",
      official_ballot_title: "Sheriff",
      candidates: [
        {
          candidate_election_id: candidateElectionId,
          candidate_id: candidateId,
          display_name: "Pat Connolly",
          fec_ids: ["H1CA00001"],
          state_filing_ids: ["SF-1"],
          records: [
            {
              id: candidateRecordId,
              description: "Handled public corruption cases.",
              research_area_tags: [{ slug: "anti_corruption", stance: "for" }],
            },
          ],
        },
      ],
      results: [{ outcome: "won", match_status: "matched" }],
    });
    expect(result.elections[1]).toMatchObject({
      id: measureElectionId,
      race_type: "ballot_measure",
      ballot_measure: {
        id: ballotMeasureId,
        source_urls: ["https://example.test/measure-h"],
        research_area_tags: [{ slug: "healthcare_affordability", stance: "for" }],
        results: [{ outcome: "passed" }],
      },
    });
    expect(query).toHaveBeenCalledTimes(9);
    expect(query.mock.calls[0]?.[1]).toEqual([[districtId]]);
  });
});
