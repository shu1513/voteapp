import { describe, expect, it, vi } from "vitest";

import { lookupAddressDistricts } from "../../../src/pipeline/address/addressDistrictLookup.js";

describe("lookupAddressDistricts", () => {
  it("returns empty output without querying when there are no keys", async () => {
    const query = vi.fn();

    await expect(lookupAddressDistricts({ query }, [])).resolves.toEqual({
      districts: [],
      missing_district_keys: [],
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("queries public.districts by district_type and geoid_compact arrays", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          id: "district-ca",
          district_type: "statewide",
          geoid_compact: "06",
          name: "California",
          state: "CA",
          state_fips: "06",
          population: 39287377,
          representation_power_score: "42.50",
        },
        {
          id: "district-la",
          district_type: "county",
          geoid_compact: "06037",
          name: "Los Angeles County",
          state: "CA",
          state_fips: "06",
          population: 9876482,
          representation_power_score: null,
        },
      ],
    });

    const result = await lookupAddressDistricts({ query }, [
      { district_type: "statewide", geoid_compact: "06" },
      { district_type: "county", geoid_compact: "06037" },
    ]);

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("FROM unnest($1::text[], $2::text[])");
    expect(query.mock.calls[0]?.[0]).toContain("JOIN public.districts AS d");
    expect(query.mock.calls[0]?.[1]).toEqual([
      ["statewide", "county"],
      ["06", "06037"],
    ]);
    expect(result).toEqual({
      districts: [
        {
          id: "district-ca",
          district_type: "statewide",
          geoid_compact: "06",
          name: "California",
          state: "CA",
          state_fips: "06",
          population: 39287377,
          representation_power_score: 42.5,
        },
        {
          id: "district-la",
          district_type: "county",
          geoid_compact: "06037",
          name: "Los Angeles County",
          state: "CA",
          state_fips: "06",
          population: 9876482,
          representation_power_score: null,
        },
      ],
      missing_district_keys: [],
    });
  });

  it("dedupes input keys and reports missing district keys", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          id: "district-la",
          district_type: "county",
          geoid_compact: "06037",
          name: "Los Angeles County",
          state: "CA",
          state_fips: "06",
          population: 9876482,
          representation_power_score: 10,
        },
      ],
    });

    const result = await lookupAddressDistricts({ query }, [
      { district_type: "county", geoid_compact: "06037" },
      { district_type: "county", geoid_compact: "06037" },
      { district_type: "place", geoid_compact: "0603666" },
    ]);

    expect(query.mock.calls[0]?.[1]).toEqual([
      ["county", "place"],
      ["06037", "0603666"],
    ]);
    expect(result.districts.map((district) => district.id)).toEqual(["district-la"]);
    expect(result.missing_district_keys).toEqual([
      { district_type: "place", geoid_compact: "0603666" },
    ]);
  });

  it("accepts keys returned by the address district resolver", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          id: "district-us-house",
          district_type: "us_house",
          geoid_compact: "0631",
          name: "Congressional District 31",
          state: "CA",
          state_fips: "06",
          population: 761000,
          representation_power_score: "73.20",
        },
      ],
    });

    const result = await lookupAddressDistricts({ query }, [
      {
        district_type: "us_house",
        geoid_compact: "0631",
        source: "mtfcc",
        layer_name: "119th Congressional Districts",
        mtfcc: "G5200",
      },
    ]);

    expect(result.districts).toEqual([
      {
        id: "district-us-house",
        district_type: "us_house",
        geoid_compact: "0631",
        name: "Congressional District 31",
        state: "CA",
        state_fips: "06",
        population: 761000,
        representation_power_score: 73.2,
      },
    ]);
  });

  it("skips blank geoid keys before querying", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });

    await lookupAddressDistricts({ query }, [
      { district_type: "county", geoid_compact: " " },
      { district_type: "statewide", geoid_compact: "06" },
    ]);

    expect(query.mock.calls[0]?.[1]).toEqual([["statewide"], ["06"]]);
  });
});
