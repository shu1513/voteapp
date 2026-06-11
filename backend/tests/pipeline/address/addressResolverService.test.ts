import { describe, expect, it, vi } from "vitest";

import { resolveAddressToDistricts } from "../../../src/pipeline/address/addressResolverService.js";

describe("resolveAddressToDistricts", () => {
  it("geocodes, resolves district keys, looks up districts, and reports missing keys", async () => {
    const geocodeAddress = vi.fn().mockResolvedValue({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
      address_match_count: 1,
      geographies: {
        Counties: [{ GEOID: "06037", NAME: "Los Angeles County", MTFCC: "G4020" }],
        "Incorporated Places": [{ GEOID: "0603666", NAME: "Baldwin Park city", MTFCC: "G4110" }],
        "2020 Census Blocks": [{ GEOID: "060374049021006", NAME: "Block 1006", MTFCC: "G5040" }],
      },
    });
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          id: "district-la",
          district_type: "county",
          geoid_compact: "06037",
          name: "Los Angeles County",
          state: "CA",
          state_fips: "06",
          population: 9876482,
          representation_power_score: "12.30",
        },
      ],
    });

    const result = await resolveAddressToDistricts(
      { query },
      "3921 Harlan Ave Baldwin Park CA 91706",
      { geocodeAddress }
    );

    expect(geocodeAddress).toHaveBeenCalledWith("3921 Harlan Ave Baldwin Park CA 91706");
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[1]).toEqual([
      ["county", "place"],
      ["06037", "0603666"],
    ]);
    expect(result).toEqual({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
      address_match_count: 1,
      district_keys: [
        {
          district_type: "county",
          geoid_compact: "06037",
          source: "mtfcc",
          layer_name: "Counties",
          mtfcc: "G4020",
          name: "Los Angeles County",
        },
        {
          district_type: "place",
          geoid_compact: "0603666",
          source: "mtfcc",
          layer_name: "Incorporated Places",
          mtfcc: "G4110",
          name: "Baldwin Park city",
        },
      ],
      districts: [
        {
          id: "district-la",
          district_type: "county",
          geoid_compact: "06037",
          name: "Los Angeles County",
          state: "CA",
          state_fips: "06",
          population: 9876482,
          representation_power_score: 12.3,
        },
      ],
      missing_district_keys: [{ district_type: "place", geoid_compact: "0603666" }],
      warnings: [],
    });
  });

  it("carries resolver warnings into the service response", async () => {
    const geocodeAddress = vi.fn().mockResolvedValue({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
      address_match_count: 1,
      geographies: {
        Counties: [{ GEOID: "06037", NAME: "Los Angeles County", MTFCC: "G5200" }],
      },
    });
    const query = vi.fn().mockResolvedValue({ rows: [] });

    const result = await resolveAddressToDistricts(
      { query },
      "3921 Harlan Ave Baldwin Park CA 91706",
      { geocodeAddress }
    );

    expect(result.district_keys).toEqual([]);
    expect(result.districts).toEqual([]);
    expect(result.warnings).toEqual([
      {
        layer_name: "Counties",
        geoid: "06037",
        mtfcc: "G5200",
        reason: "MTFCC maps to us_house but layer name maps to county",
      },
    ]);
  });

  it("uses cached geocode resolution and still looks up current districts", async () => {
    const geocodeAddress = vi.fn();
    const cache = {
      get: vi.fn(async () =>
        JSON.stringify({
          matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
          coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
          address_match_count: 1,
          district_keys: [
            {
              district_type: "county",
              geoid_compact: "06037",
              source: "mtfcc",
              layer_name: "Counties",
              mtfcc: "G4020",
              name: "Los Angeles County",
            },
          ],
          warnings: [],
          cached_at: "2026-06-08T00:00:00.000Z",
        })
      ),
      set: vi.fn(),
    };
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          id: "district-la",
          district_type: "county",
          geoid_compact: "06037",
          name: "Los Angeles County",
          state: "CA",
          state_fips: "06",
          population: 9876482,
          representation_power_score: "12.30",
        },
      ],
    });

    const result = await resolveAddressToDistricts({ query }, "3921 Harlan Ave Baldwin Park CA 91706", {
      geocodeAddress,
      cache,
      geocoderOptions: { benchmark: "Public_AR_Current", vintage: "ACS2024_Current" },
    });

    expect(geocodeAddress).not.toHaveBeenCalled();
    expect(cache.set).not.toHaveBeenCalled();
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[1]).toEqual([["county"], ["06037"]]);
    expect(result.districts).toEqual([
      {
        id: "district-la",
        district_type: "county",
        geoid_compact: "06037",
        name: "Los Angeles County",
        state: "CA",
        state_fips: "06",
        population: 9876482,
        representation_power_score: 12.3,
      },
    ]);
  });

  it("writes cache on miss using configured TTL", async () => {
    const geocodeAddress = vi.fn().mockResolvedValue({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
      address_match_count: 1,
      geographies: {
        Counties: [{ GEOID: "06037", NAME: "Los Angeles County", MTFCC: "G4020" }],
      },
    });
    const cache = {
      get: vi.fn(async () => null),
      set: vi.fn(async () => "OK"),
    };
    const query = vi.fn().mockResolvedValue({ rows: [] });

    await resolveAddressToDistricts({ query }, "3921 Harlan Ave Baldwin Park CA 91706", {
      geocodeAddress,
      cache,
      cacheTtlSeconds: 123,
    });

    expect(geocodeAddress).toHaveBeenCalledOnce();
    expect(cache.set).toHaveBeenCalledWith(expect.stringMatching(/^address_lookup:v1:[a-f0-9]{64}$/), expect.any(String), {
      EX: 123,
    });
    const cachedPayload = JSON.parse(String(cache.set.mock.calls[0]?.[1]));
    expect(cachedPayload).toMatchObject({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      district_keys: [{ district_type: "county", geoid_compact: "06037" }],
    });
    expect(JSON.stringify(cachedPayload)).not.toContain("3921 Harlan Ave Baldwin Park CA 91706");
  });

  it("falls back to live geocoding when cache read or write fails", async () => {
    const geocodeAddress = vi.fn().mockResolvedValue({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
      address_match_count: 1,
      geographies: {
        Counties: [{ GEOID: "06037", NAME: "Los Angeles County", MTFCC: "G4020" }],
      },
    });
    const cache = {
      get: vi.fn(async () => {
        throw new Error("redis get failed");
      }),
      set: vi.fn(async () => {
        throw new Error("redis set failed");
      }),
    };
    const query = vi.fn().mockResolvedValue({ rows: [] });

    const result = await resolveAddressToDistricts({ query }, "3921 Harlan Ave Baldwin Park CA 91706", {
      geocodeAddress,
      cache,
    });

    expect(result.district_keys).toEqual([
      expect.objectContaining({ district_type: "county", geoid_compact: "06037" }),
    ]);
    expect(geocodeAddress).toHaveBeenCalledOnce();
    expect(cache.set).toHaveBeenCalledOnce();
  });
});
