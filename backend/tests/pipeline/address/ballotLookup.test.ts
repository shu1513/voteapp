import { afterEach, describe, expect, it, vi } from "vitest";

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

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

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
      })
      .mockResolvedValueOnce({
        rows: [],
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
    expect(query).toHaveBeenCalledTimes(7);
    expect(query.mock.calls[0]?.[1]).toEqual([[districtId]]);
    expect(query.mock.calls[5]?.[0]).toContain("CASE pass_type");
    expect(query.mock.calls[5]?.[0]).toContain("WHEN 'certified' THEN 1");
    expect(query.mock.calls[5]?.[0]).toContain("WHEN 'election_night' THEN 2");
    expect(query.mock.calls[6]?.[0]).toContain("historical_contest_margins");
    expect(JSON.stringify(result)).not.toContain("candidates");
    expect(JSON.stringify(result)).not.toContain("candidate_record");
    expect(JSON.stringify(result)).not.toContain("what_yes_means");
  });

  it("attaches historical competitiveness to supported office summaries", async () => {
    const fetch = vi.fn().mockRejectedValue(new Error("runtime ballot lookup must not fetch historical data"));
    vi.stubGlobal("fetch", fetch);
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
          {
            id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            lookup_id: houseElectionId,
            source: "MIT_2022",
            source_url: "https://doi.org/10.7910/DVN/UYQIEP",
            election_year: 2022,
            state: "CA",
            state_fips: "06",
            office_type: "US_HOUSE",
            district_type: "us_house",
            district_key: "0631",
            mit_office: "US HOUSE",
            mit_district: "031",
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            winner_votes: "115200",
            runner_up_votes: "84800",
            total_votes: "200000",
            margin_percent: "15.20",
            competitiveness_label: "somewhat_competitive",
            stale_after_redistricting: false,
            imported_at: "2026-06-14 11:00:00+00",
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
        display_label: "Historically somewhat competitive",
        display_description: "Based on weighted margins from 2024 and 2022 U.S. House results.",
        source: "MIT_2024",
        source_url: "https://github.com/MEDSL/2024-elections-official",
        election_year: 2024,
        winner_party: "DEMOCRAT",
        runner_up_party: "REPUBLICAN",
        margin_percent: 11.45,
        competitiveness_label: "somewhat_competitive",
        stale_after_redistricting: false,
        method: "weighted_last_3",
        weights: [0.625, 0.375],
        election_years: [2024, 2022],
        contests_used: [
          {
            source: "MIT_2024",
            source_url: "https://github.com/MEDSL/2024-elections-official",
            election_year: 2024,
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            margin_percent: 9.2,
            competitiveness_label: "competitive",
            stale_after_redistricting: false,
            weight: 0.625,
          },
          {
            source: "MIT_2022",
            source_url: "https://doi.org/10.7910/DVN/UYQIEP",
            election_year: 2022,
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            margin_percent: 15.2,
            competitiveness_label: "somewhat_competitive",
            stale_after_redistricting: false,
            weight: 0.375,
          },
        ],
      },
    });
    expect(query).toHaveBeenCalledTimes(7);
    expect(fetch).not.toHaveBeenCalled();
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

  it("attaches historical competitiveness to supported county office summaries", async () => {
    const countyDistrictId = "77777777-7777-4777-8777-777777777771";
    const sheriffElectionId = "77777777-7777-4777-8777-777777777772";
    const sheriffOfficeId = "77777777-7777-4777-8777-777777777773";
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: countyDistrictId,
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
            election_id: sheriffElectionId,
            district_id: countyDistrictId,
            district_type: "county",
            geoid_compact: "06037",
            district_name: "Los Angeles County",
            state: "CA",
            state_fips: "06",
            race_type: "office",
            official_ballot_title: "Sheriff",
            election_date: "2026-11-03",
            election_stage: "general",
            is_partisan: false,
            discovery_contest_family: "non_judicial_office",
            sources: ["https://example.test/elections"],
            office_id: sheriffOfficeId,
            office_scope: "county",
            office_canonical_name: "Sheriff",
            office_summary: "County law-enforcement executive.",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ election_id: sheriffElectionId, candidate_count: 2 }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [
          {
            id: "77777777-7777-4777-8777-777777777774",
            lookup_id: sheriffElectionId,
            source: "MIT_2022",
            source_url: "https://doi.org/10.7910/DVN/UYQIEP",
            election_year: 2022,
            state: "CA",
            state_fips: "06",
            office_type: "COUNTY_SHERIFF",
            district_type: "county",
            district_key: "06037",
            mit_office: "COUNTY SHERIFF",
            mit_district: "06037",
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            winner_votes: "56000",
            runner_up_votes: "44000",
            total_votes: "100000",
            margin_percent: "12.00",
            competitiveness_label: "somewhat_competitive",
            stale_after_redistricting: false,
            imported_at: "2026-06-14 12:00:00+00",
          },
          {
            id: "77777777-7777-4777-8777-777777777775",
            lookup_id: sheriffElectionId,
            source: "MIT_2018",
            source_url: "https://doi.org/10.7910/DVN/UBKYRU",
            election_year: 2018,
            state: "CA",
            state_fips: "06",
            office_type: "COUNTY_SHERIFF",
            district_type: "county",
            district_key: "06037",
            mit_office: "COUNTY SHERIFF",
            mit_district: "06037",
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            winner_votes: "52000",
            runner_up_votes: "48000",
            total_votes: "100000",
            margin_percent: "4.00",
            competitiveness_label: "very_competitive",
            stale_after_redistricting: false,
            imported_at: "2026-06-14 11:00:00+00",
          },
        ],
      });

    const result = await lookupBallotSummariesByDistrictIds({ query }, [countyDistrictId]);

    expect(result.elections).toHaveLength(1);
    expect(result.elections[0]).toMatchObject({
      id: sheriffElectionId,
      district: {
        district_type: "county",
        geoid_compact: "06037",
      },
      office: {
        canonical_name: "Sheriff",
      },
      historical_competitiveness: {
        display_label: "Historically competitive",
        display_description: "Based on weighted margins from 2022 and 2018 Sheriff results.",
        source: "MIT_2022",
        source_url: "https://doi.org/10.7910/DVN/UYQIEP",
        election_year: 2022,
        winner_party: "DEMOCRAT",
        runner_up_party: "REPUBLICAN",
        margin_percent: 9,
        competitiveness_label: "competitive",
        stale_after_redistricting: false,
        method: "weighted_last_3",
        weights: [0.625, 0.375],
        election_years: [2022, 2018],
        contests_used: [
          {
            source: "MIT_2022",
            source_url: "https://doi.org/10.7910/DVN/UYQIEP",
            election_year: 2022,
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            margin_percent: 12,
            competitiveness_label: "somewhat_competitive",
            stale_after_redistricting: false,
            weight: 0.625,
          },
          {
            source: "MIT_2018",
            source_url: "https://doi.org/10.7910/DVN/UBKYRU",
            election_year: 2018,
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            margin_percent: 4,
            competitiveness_label: "very_competitive",
            stale_after_redistricting: false,
            weight: 0.375,
          },
        ],
      },
    });
    expect(query).toHaveBeenCalledTimes(7);
    expect(JSON.parse(query.mock.calls[6]?.[1]?.[0] as string)).toEqual([
      {
        lookup_id: sheriffElectionId,
        min_election_year: null,
        max_election_year: 2025,
        state: "CA",
        state_fips: "06",
        office_type: "COUNTY_SHERIFF",
        district_type: "county",
        district_key: "06037",
        mit_office: "COUNTY SHERIFF",
        mit_district: "06037",
      },
    ]);
  });

  it("attaches county office historical competitiveness using each county district key", async () => {
    const counties = [
      {
        districtId: "77777777-7777-4777-8777-7777777777a1",
        electionId: "77777777-7777-4777-8777-7777777777a2",
        officeId: "77777777-7777-4777-8777-7777777777a3",
        geoidCompact: "06037",
        name: "Los Angeles County",
        state: "CA",
        stateFips: "06",
        margin: "12.00",
        label: "somewhat_competitive",
      },
      {
        districtId: "77777777-7777-4777-8777-7777777777b1",
        electionId: "77777777-7777-4777-8777-7777777777b2",
        officeId: "77777777-7777-4777-8777-7777777777b3",
        geoidCompact: "53033",
        name: "King County",
        state: "WA",
        stateFips: "53",
        margin: "4.00",
        label: "very_competitive",
      },
      {
        districtId: "77777777-7777-4777-8777-7777777777c1",
        electionId: "77777777-7777-4777-8777-7777777777c2",
        officeId: "77777777-7777-4777-8777-7777777777c3",
        geoidCompact: "27053",
        name: "Hennepin County",
        state: "MN",
        stateFips: "27",
        margin: "22.00",
        label: "safe",
      },
    ] as const;
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: counties.map((county) => ({
          id: county.districtId,
          district_type: "county",
          geoid_compact: county.geoidCompact,
          name: county.name,
          state: county.state,
          state_fips: county.stateFips,
        })),
      })
      .mockResolvedValueOnce({
        rows: counties.map((county) => ({
          election_id: county.electionId,
          district_id: county.districtId,
          district_type: "county",
          geoid_compact: county.geoidCompact,
          district_name: county.name,
          state: county.state,
          state_fips: county.stateFips,
          race_type: "office",
          official_ballot_title: "Sheriff",
          election_date: "2026-11-03",
          election_stage: "general",
          is_partisan: false,
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.test/elections"],
          office_id: county.officeId,
          office_scope: "county",
          office_canonical_name: "Sheriff",
          office_summary: "County law-enforcement executive.",
        })),
      })
      .mockResolvedValueOnce({
        rows: counties.map((county) => ({ election_id: county.electionId, candidate_count: 2 })),
      })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: counties.map((county, index) => ({
          id: `77777777-7777-4777-8777-7777777778${index}1`,
          lookup_id: county.electionId,
          source: "MIT_2022",
          source_url: "https://doi.org/10.7910/DVN/UYQIEP",
          election_year: 2022,
          state: county.state,
          state_fips: county.stateFips,
          office_type: "COUNTY_SHERIFF",
          district_type: "county",
          district_key: county.geoidCompact,
          mit_office: "COUNTY SHERIFF",
          mit_district: county.geoidCompact,
          winner_party: "DEMOCRAT",
          runner_up_party: "REPUBLICAN",
          winner_votes: "56000",
          runner_up_votes: "44000",
          total_votes: "100000",
          margin_percent: county.margin,
          competitiveness_label: county.label,
          stale_after_redistricting: false,
          imported_at: "2026-06-14 12:00:00+00",
        })),
      });

    const result = await lookupBallotSummariesByDistrictIds(
      { query },
      counties.map((county) => county.districtId)
    );

    expect(result.elections).toHaveLength(3);
    for (const county of counties) {
      expect(result.elections).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            id: county.electionId,
            district: expect.objectContaining({
              district_type: "county",
              geoid_compact: county.geoidCompact,
            }),
            office: expect.objectContaining({
              canonical_name: "Sheriff",
            }),
            historical_competitiveness: expect.objectContaining({
              source: "MIT_2022",
              election_year: 2022,
              margin_percent: Number(county.margin),
              competitiveness_label: county.label,
              election_years: [2022],
            }),
          }),
        ])
      );
    }
    expect(JSON.parse(query.mock.calls[6]?.[1]?.[0] as string)).toEqual(
      counties.map((county) => ({
        lookup_id: county.electionId,
        min_election_year: null,
        max_election_year: 2025,
        state: county.state,
        state_fips: county.stateFips,
        office_type: "COUNTY_SHERIFF",
        district_type: "county",
        district_key: county.geoidCompact,
        mit_office: "COUNTY SHERIFF",
        mit_district: county.geoidCompact,
      }))
    );
  });

  it("does not look up historical competitiveness for unsupported county office summaries", async () => {
    const countyDistrictId = "77777777-7777-4777-8777-777777777781";
    const commissionerElectionId = "77777777-7777-4777-8777-777777777782";
    const commissionerOfficeId = "77777777-7777-4777-8777-777777777783";
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: countyDistrictId,
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
            election_id: commissionerElectionId,
            district_id: countyDistrictId,
            district_type: "county",
            geoid_compact: "06037",
            district_name: "Los Angeles County",
            state: "CA",
            state_fips: "06",
            race_type: "office",
            official_ballot_title: "County Commissioner",
            election_date: "2026-11-03",
            election_stage: "general",
            is_partisan: false,
            discovery_contest_family: "non_judicial_office",
            sources: ["https://example.test/elections"],
            office_id: commissionerOfficeId,
            office_scope: "county",
            office_canonical_name: "County Commissioner",
            office_summary: "County board member.",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ election_id: commissionerElectionId, candidate_count: 2 }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });

    const result = await lookupBallotSummariesByDistrictIds({ query }, [countyDistrictId]);

    expect(result.elections).toHaveLength(1);
    expect(result.elections[0]).toMatchObject({
      id: commissionerElectionId,
      office: {
        canonical_name: "County Commissioner",
      },
      historical_competitiveness: null,
    });
    expect(query).toHaveBeenCalledTimes(6);
    expect(query.mock.calls.map((call) => call[0]).join("\n")).not.toContain("historical_contest_margins");
  });

  it("attaches historical competitiveness to supported statewide executive office summaries", async () => {
    const statewideDistrictId = "88888888-8888-4888-8888-888888888881";
    const attorneyGeneralElectionId = "88888888-8888-4888-8888-888888888882";
    const attorneyGeneralOfficeId = "88888888-8888-4888-8888-888888888883";
    const fetch = vi.fn().mockRejectedValue(new Error("runtime ballot lookup must not fetch historical data"));
    vi.stubGlobal("fetch", fetch);
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: statewideDistrictId,
            district_type: "statewide",
            geoid_compact: "06",
            name: "California",
            state: "CA",
            state_fips: "06",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: attorneyGeneralElectionId,
            district_id: statewideDistrictId,
            district_type: "statewide",
            geoid_compact: "06",
            district_name: "California",
            state: "CA",
            state_fips: "06",
            race_type: "office",
            official_ballot_title: "Attorney General",
            election_date: "2026-11-03",
            election_stage: "general",
            is_partisan: true,
            discovery_contest_family: "non_judicial_office",
            sources: ["https://example.test/elections"],
            office_id: attorneyGeneralOfficeId,
            office_scope: "statewide",
            office_canonical_name: "Attorney General",
            office_summary: "State chief legal officer.",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [{ election_id: attorneyGeneralElectionId, candidate_count: 2 }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [
          {
            id: "88888888-8888-4888-8888-888888888884",
            lookup_id: attorneyGeneralElectionId,
            source: "MIT_2022",
            source_url: "https://doi.org/10.7910/DVN/UYQIEP",
            election_year: 2022,
            state: "CA",
            state_fips: "06",
            office_type: "ATTORNEY_GENERAL",
            district_type: "statewide",
            district_key: "06",
            mit_office: "ATTORNEY GENERAL",
            mit_district: "STATEWIDE",
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            winner_votes: "1035000",
            runner_up_votes: "1000000",
            total_votes: "2035000",
            margin_percent: "1.72",
            competitiveness_label: "toss_up",
            stale_after_redistricting: false,
            imported_at: "2026-06-14 12:00:00+00",
          },
        ],
      });

    const result = await lookupBallotSummariesByDistrictIds({ query }, [statewideDistrictId]);

    expect(result.elections).toHaveLength(1);
    expect(result.elections[0]).toMatchObject({
      id: attorneyGeneralElectionId,
      district: {
        district_type: "statewide",
        geoid_compact: "06",
      },
      office: {
        canonical_name: "Attorney General",
      },
      historical_competitiveness: {
        display_label: "Historically a toss-up",
        display_description: "Based on the 2022 Attorney General result.",
        source: "MIT_2022",
        source_url: "https://doi.org/10.7910/DVN/UYQIEP",
        election_year: 2022,
        winner_party: "DEMOCRAT",
        runner_up_party: "REPUBLICAN",
        margin_percent: 1.72,
        competitiveness_label: "toss_up",
        stale_after_redistricting: false,
        method: "weighted_last_3",
        weights: [1],
        election_years: [2022],
        contests_used: [
          {
            source: "MIT_2022",
            source_url: "https://doi.org/10.7910/DVN/UYQIEP",
            election_year: 2022,
            winner_party: "DEMOCRAT",
            runner_up_party: "REPUBLICAN",
            margin_percent: 1.72,
            competitiveness_label: "toss_up",
            stale_after_redistricting: false,
            weight: 1,
          },
        ],
      },
    });
    expect(query).toHaveBeenCalledTimes(7);
    expect(fetch).not.toHaveBeenCalled();
    expect(JSON.parse(query.mock.calls[6]?.[1]?.[0] as string)).toEqual([
      {
        lookup_id: attorneyGeneralElectionId,
        min_election_year: null,
        max_election_year: 2025,
        state: "CA",
        state_fips: "06",
        office_type: "ATTORNEY_GENERAL",
        district_type: "statewide",
        district_key: "06",
        mit_office: "ATTORNEY GENERAL",
        mit_district: "STATEWIDE",
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
