import { describe, expect, it, vi } from "vitest";

import {
  lookupHistoricalContestMargins,
} from "../../../src/pipeline/competitiveness/historicalContestMarginLookup.js";

describe("historicalContestMarginLookup", () => {
  it("does not query when no inputs can be mapped to historical contest keys", async () => {
    const query = vi.fn();

    const result = await lookupHistoricalContestMargins({ query } as never, [
      {
        lookupId: "county-sheriff",
        officeCanonicalName: "Sheriff",
        districtType: "county",
        geoidCompact: "06037",
        stateFips: "06",
      },
      {
        lookupId: "blank",
        officeCanonicalName: "United States Representative",
        districtType: "county",
        geoidCompact: "06037",
        stateFips: "06",
      },
    ]);

    expect(result.size).toBe(0);
    expect(query).not.toHaveBeenCalled();
  });

  it("builds batch lookup keys for supported current elections", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });

    await lookupHistoricalContestMargins({ query } as never, [
      {
        lookupId: "house-ca-31",
        officeCanonicalName: "United States Representative",
        districtType: "us_house",
        geoidCompact: "0631",
        stateFips: "06",
        currentElectionYear: 2026,
        maxElectionYear: 2024,
      },
      {
        lookupId: "ca-senate",
        officeCanonicalName: "United States Senator",
        districtType: "statewide",
        geoidCompact: "06",
        stateFips: "06",
      },
    ]);

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("jsonb_to_recordset");
    expect(JSON.parse(query.mock.calls[0]?.[1]?.[0] as string)).toEqual([
      {
        lookup_id: "house-ca-31",
        min_election_year: 2022,
        max_election_year: 2024,
        state: "CA",
        state_fips: "06",
        office_type: "US_HOUSE",
        district_type: "us_house",
        district_key: "0631",
        mit_office: "US HOUSE",
        mit_district: "031",
      },
      {
        lookup_id: "ca-senate",
        min_election_year: null,
        max_election_year: null,
        state: "CA",
        state_fips: "06",
        office_type: "US_SENATE",
        district_type: "statewide",
        district_key: "06",
        mit_office: "US SENATE",
        mit_district: "STATEWIDE",
      },
    ]);
  });

  it("filters district history to the same post-redistricting cycle", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });

    await lookupHistoricalContestMargins({ query } as never, [
      {
        lookupId: "house-ca-31-2032",
        officeCanonicalName: "United States Representative",
        districtType: "us_house",
        geoidCompact: "0631",
        stateFips: "06",
        currentElectionYear: 2032,
        maxElectionYear: 2031,
      },
    ]);

    expect(query.mock.calls[0]?.[0]).toContain("hcm.election_year >= key.min_election_year");
    expect(JSON.parse(query.mock.calls[0]?.[1]?.[0] as string)).toEqual([
      {
        lookup_id: "house-ca-31-2032",
        min_election_year: 2032,
        max_election_year: 2031,
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

  it("returns matched historical margin rows keyed by lookup ID", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
          lookup_id: "house-ca-31",
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

    const result = await lookupHistoricalContestMargins({ query } as never, [
      {
        lookupId: "house-ca-31",
        officeCanonicalName: "United States Representative",
        districtType: "us_house",
        geoidCompact: "0631",
        stateFips: "06",
      },
    ]);

    expect(result.get("house-ca-31")).toEqual({
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      lookup_id: "house-ca-31",
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
      winner_votes: 109_200,
      runner_up_votes: 90_800,
      total_votes: 200_000,
      margin_percent: 9.2,
      competitiveness_label: "competitive",
      stale_after_redistricting: false,
      imported_at: "2026-06-14 12:00:00+00",
    });
  });

  it("dedupes repeated lookup IDs after trimming", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });

    await lookupHistoricalContestMargins({ query } as never, [
      {
        lookupId: " house-ca-31 ",
        officeCanonicalName: "United States Representative",
        districtType: "us_house",
        geoidCompact: "0631",
        stateFips: "06",
      },
      {
        lookupId: "house-ca-31",
        officeCanonicalName: "United States Representative",
        districtType: "us_house",
        geoidCompact: "0631",
        stateFips: "06",
      },
    ]);

    expect(JSON.parse(query.mock.calls[0]?.[1]?.[0] as string)).toHaveLength(1);
  });
});
