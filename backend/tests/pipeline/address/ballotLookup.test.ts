import { describe, expect, it, vi } from "vitest";

import {
  lookupBallotSummariesByDistrictIds,
  lookupElectionDetailById,
} from "../../../src/pipeline/address/ballotLookup.js";

const districtId = "11111111-1111-4111-8111-111111111111";
const officeElectionId = "22222222-2222-4222-8222-222222222222";
const measureElectionId = "33333333-3333-4333-8333-333333333333";
const officeId = "12121212-1212-4212-8212-121212121212";
const candidateId = "44444444-4444-4444-8444-444444444444";
const candidateElectionId = "55555555-5555-4555-8555-555555555555";
const candidateRecordId = "66666666-6666-4666-8666-666666666666";
const ballotMeasureId = "77777777-7777-4777-8777-777777777777";
const researchAreaId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";

describe("lookupBallotSummariesByDistrictIds", () => {
  it("returns an empty summary result without querying for empty district IDs", async () => {
    const db = { query: vi.fn() };

    await expect(lookupBallotSummariesByDistrictIds(db, [" "])).resolves.toEqual({
      district_ids: [],
      districts: [],
      elections: [],
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("loads lightweight election summaries with office context, area links, counts, and result status", async () => {
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
            office_id: officeId,
            office_scope: "county",
            office_canonical_name: "Sheriff",
            office_summary: "County law-enforcement executive.",
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
            office_id: null,
            office_scope: null,
            office_canonical_name: null,
            office_summary: null,
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [{ election_id: officeElectionId, candidate_count: 3 }],
      })
      .mockResolvedValueOnce({
        rows: [{ election_id: measureElectionId, ballot_measure_id: ballotMeasureId }],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            office_id: officeId,
            research_area_id: researchAreaId,
            slug: "public_safety_and_crime_control",
            name: "Public Safety and Crime Control",
            description: "Crime, policing, and public safety.",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: officeElectionId,
            outcome: "won",
          },
        ],
      });

    const result = await lookupBallotSummariesByDistrictIds({ query }, [districtId, districtId]);

    expect(result).toEqual({
      district_ids: [districtId],
      districts: [
        {
          id: districtId,
          district_type: "county",
          geoid_compact: "06037",
          name: "Los Angeles County",
          state: "CA",
          state_fips: "06",
        },
      ],
      elections: [
        {
          id: officeElectionId,
          district_id: districtId,
          district: {
            id: districtId,
            district_type: "county",
            geoid_compact: "06037",
            name: "Los Angeles County",
            state: "CA",
            state_fips: "06",
          },
          race_type: "office",
          official_ballot_title: "Sheriff",
          election_date: "2026-06-02",
          election_stage: "primary",
          is_partisan: false,
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.test/elections"],
          candidate_count: 3,
          ballot_measure_id: null,
          has_results: true,
          current_result_outcome: "won",
          office: {
            id: officeId,
            scope: "county",
            canonical_name: "Sheriff",
            summary: "County law-enforcement executive.",
          },
          research_areas: [
            {
              id: researchAreaId,
              slug: "public_safety_and_crime_control",
              name: "Public Safety and Crime Control",
              description: "Crime, policing, and public safety.",
            },
          ],
          historical_competitiveness: null,
        },
        {
          id: measureElectionId,
          district_id: districtId,
          district: {
            id: districtId,
            district_type: "county",
            geoid_compact: "06037",
            name: "Los Angeles County",
            state: "CA",
            state_fips: "06",
          },
          race_type: "ballot_measure",
          official_ballot_title: "Measure H",
          election_date: "2026-06-02",
          election_stage: null,
          is_partisan: null,
          discovery_contest_family: "ballot_measure",
          sources: ["https://example.test/measure"],
          candidate_count: 0,
          ballot_measure_id: ballotMeasureId,
          has_results: false,
          current_result_outcome: null,
          office: null,
          research_areas: [],
          historical_competitiveness: null,
        },
      ],
    });
    expect(query).toHaveBeenCalledTimes(6);
    expect(query.mock.calls[0]?.[1]).toEqual([[districtId]]);
    expect(query.mock.calls[5]?.[0]).toContain("CASE pass_type");
    expect(query.mock.calls[5]?.[0]).toContain("WHEN 'certified' THEN 1");
    expect(query.mock.calls[5]?.[0]).toContain("WHEN 'election_night' THEN 2");
    expect(JSON.stringify(result)).not.toContain("candidates");
    expect(JSON.stringify(result)).not.toContain("candidate_record");
    expect(JSON.stringify(result)).not.toContain("what_yes_means");
  });

  it("attaches historical competitiveness to supported office summaries", async () => {
    const houseDistrictId = "99999999-9999-4999-8999-999999999991";
    const houseElectionId = "99999999-9999-4999-8999-999999999992";
    const houseOfficeId = "99999999-9999-4999-8999-999999999993";
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: houseDistrictId,
            district_type: "us_house",
            geoid_compact: "0631",
            name: "California's 31st Congressional District",
            state: "CA",
            state_fips: "06",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: houseElectionId,
            district_id: houseDistrictId,
            district_type: "us_house",
            geoid_compact: "0631",
            district_name: "California's 31st Congressional District",
            state: "CA",
            state_fips: "06",
            race_type: "office",
            official_ballot_title: "United States Representative District 31",
            election_date: "2026-11-03",
            election_stage: "general",
            is_partisan: true,
            discovery_contest_family: "non_judicial_office",
            sources: ["https://example.test/elections"],
            office_id: houseOfficeId,
            office_scope: "us_house",
            office_canonical_name: "United States Representative",
            office_summary: "Federal lower-chamber legislator.",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ election_id: houseElectionId, candidate_count: 2 }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [
          {
            id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            lookup_id: houseElectionId,
            source: "MIT_2024",
            source_url: "https://github.com/MEDSL/2024-elections-official",
            election_year: 2024,
            state: "CA",
            state_fips: "06",
            office_type: "US_HOUSE",
            district_type: "us_house",
            district_key: "0631",
            mit_office: "US HOUSE",
            mit_district: "031",
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            winner_votes: "109200",
            runner_up_votes: "90800",
            total_votes: "200000",
            margin_percent: "9.20",
            competitiveness_label: "competitive",
            stale_after_redistricting: false,
            imported_at: "2026-06-14 12:00:00+00",
          },
        ],
      });

    const result = await lookupBallotSummariesByDistrictIds({ query }, [houseDistrictId]);

    expect(result.elections).toHaveLength(1);
    expect(result.elections[0]).toMatchObject({
      id: houseElectionId,
      office: {
        canonical_name: "United States Representative",
      },
      historical_competitiveness: {
        source: "MIT_2024",
        source_url: "https://github.com/MEDSL/2024-elections-official",
        election_year: 2024,
        winner_party: "DEMOCRAT",
        runner_up_party: "REPUBLICAN",
        margin_percent: 9.2,
        competitiveness_label: "competitive",
        stale_after_redistricting: false,
      },
    });
    expect(query).toHaveBeenCalledTimes(7);
    expect(query.mock.calls[6]?.[0]).toContain("public.historical_contest_margins");
    expect(JSON.parse(query.mock.calls[6]?.[1]?.[0] as string)).toEqual([
      {
        lookup_id: houseElectionId,
        min_election_year: 2022,
        max_election_year: 2025,
        state: "CA",
        state_fips: "06",
        office_type: "US_HOUSE",
        district_type: "us_house",
        district_key: "0631",
        mit_office: "US HOUSE",
        mit_district: "031",
      },
    ]);
  });
});

describe("lookupElectionDetailById", () => {
  it("returns null without querying for empty election IDs", async () => {
    const query = vi.fn();

    await expect(lookupElectionDetailById({ query }, "   ")).resolves.toBeNull();
    expect(query).not.toHaveBeenCalled();
  });

  it("returns null without detail queries when the election does not exist", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });

    await expect(lookupElectionDetailById({ query }, officeElectionId)).resolves.toBeNull();
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("loads full detail for one office election by election ID", async () => {
    const query = vi
      .fn()
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
      .mockResolvedValueOnce({ rows: [] })
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
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] })
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
            research_area_id: researchAreaId,
            slug: "anti_corruption",
            name: "Anti-Corruption",
            stance: "for",
          },
        ],
      });

    const result = await lookupElectionDetailById({ query }, officeElectionId);

    expect(result).toMatchObject({
      id: officeElectionId,
      district_id: districtId,
      race_type: "office",
      official_ballot_title: "Sheriff",
      candidates: [
        {
          candidate_election_id: candidateElectionId,
          candidate_id: candidateId,
          display_name: "Pat Connolly",
          records: [
            {
              id: candidateRecordId,
              description: "Handled public corruption cases.",
              research_area_tags: [{ slug: "anti_corruption", stance: "for" }],
            },
          ],
        },
      ],
      ballot_measure: null,
      results: [{ outcome: "won", match_status: "matched" }],
    });
    expect(query).toHaveBeenCalledTimes(7);
    expect(query.mock.calls[0]?.[1]).toEqual([officeElectionId]);
  });

  it("loads full detail for one ballot measure election by election ID", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
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
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: measureElectionId,
            ballot_measure_id: ballotMeasureId,
            official_ballot_title: "Measure H",
            summary: "Funds hospitals.",
            what_yes_means: "Raises the tax.",
            what_no_means: "Keeps current tax rates.",
            result: "passed",
            source_url: ["https://example.test/measure-h"],
            official_measure_url: "https://example.test/measure-h/full-text",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] })
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
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            ballot_measure_id: ballotMeasureId,
            research_area_id: researchAreaId,
            slug: "healthcare_affordability",
            name: "Healthcare Affordability",
            stance: "for",
          },
        ],
      });

    const result = await lookupElectionDetailById({ query }, measureElectionId);

    expect(result).toMatchObject({
      id: measureElectionId,
      district_id: districtId,
      race_type: "ballot_measure",
      official_ballot_title: "Measure H",
      candidates: [],
      results: [],
      ballot_measure: {
        id: ballotMeasureId,
        summary: "Funds hospitals.",
        what_yes_means: "Raises the tax.",
        what_no_means: "Keeps current tax rates.",
        result: "passed",
        source_urls: ["https://example.test/measure-h"],
        official_measure_url: "https://example.test/measure-h/full-text",
        research_area_tags: [{ slug: "healthcare_affordability", stance: "for" }],
        results: [{ outcome: "passed", result_status: "certified" }],
      },
    });
    expect(query).toHaveBeenCalledTimes(6);
    expect(query.mock.calls[0]?.[1]).toEqual([measureElectionId]);
  });
});
