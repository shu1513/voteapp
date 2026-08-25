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
          requested_district_type: "county",
          requested_geoid_compact: "06037",
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
          requested_district_type: "county",
          requested_geoid_compact: "06037",
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
    expect(cache.set).toHaveBeenCalledWith(expect.stringMatching(/^address_lookup:v2:[a-f0-9]{64}$/), expect.any(String), {
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

describe("resolveAddressToDistricts with coordinates", () => {
  const GEOGRAPHIES = {
    Counties: [{ GEOID: "34003", NAME: "Bergen County", MTFCC: "G4020" }],
  };
  const DISTRICT_ROW = {
    id: "district-bergen",
    district_type: "county",
    geoid_compact: "34003",
    name: "Bergen County",
    state: "NJ",
    state_fips: "34",
    population: 955732,
    representation_power_score: "10.00",
    requested_district_type: "county",
    requested_geoid_compact: "34003",
  };
  const COORDINATES = { lat: 40.8135, lng: -74.0741 };

  it("resolves by point, never geocodes the string, and never touches the cache", async () => {
    const geocodeAddress = vi.fn();
    const geocodeCoordinates = vi.fn().mockResolvedValue({ geographies: GEOGRAPHIES });
    const cache = { get: vi.fn(), set: vi.fn() };
    const query = vi.fn().mockResolvedValue({ rows: [DISTRICT_ROW] });

    const result = await resolveAddressToDistricts(
      { query },
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA",
      { geocodeAddress, geocodeCoordinates, cache, coordinates: COORDINATES }
    );

    expect(geocodeCoordinates).toHaveBeenCalledWith(COORDINATES);
    expect(geocodeAddress).not.toHaveBeenCalled();
    expect(cache.get).not.toHaveBeenCalled();
    expect(cache.set).not.toHaveBeenCalled();
    expect(result.matched_address).toBe("1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA");
    expect(result.coordinates).toEqual(COORDINATES);
    expect(result.address_match_count).toBe(1);
    expect(result.districts).toEqual([
      expect.objectContaining({ id: "district-bergen", geoid_compact: "34003" }),
    ]);
  });

  it("falls back to the address-string path when the point lookup fails", async () => {
    const { CensusAddressGeocoderError } = await import("../../../src/pipeline/address/censusAddressGeocoder.js");
    const geocodeCoordinates = vi.fn().mockRejectedValue(new CensusAddressGeocoderError("timeout", "slow"));
    const geocodeAddress = vi.fn().mockResolvedValue({
      matched_address: "1 METLIFE STADIUM DR, EAST RUTHERFORD, NJ, 07073",
      coordinates: COORDINATES,
      address_match_count: 1,
      geographies: GEOGRAPHIES,
    });
    const query = vi.fn().mockResolvedValue({ rows: [DISTRICT_ROW] });

    const result = await resolveAddressToDistricts(
      { query },
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA",
      { geocodeAddress, geocodeCoordinates, coordinates: COORDINATES }
    );

    expect(geocodeCoordinates).toHaveBeenCalledOnce();
    expect(geocodeAddress).toHaveBeenCalledOnce();
    expect(result.matched_address).toBe("1 METLIFE STADIUM DR, EAST RUTHERFORD, NJ, 07073");
  });

  it("falls back to the address-string path when the point matches no district keys", async () => {
    const geocodeCoordinates = vi.fn().mockResolvedValue({ geographies: {} });
    const geocodeAddress = vi.fn().mockResolvedValue({
      matched_address: "1 METLIFE STADIUM DR, EAST RUTHERFORD, NJ, 07073",
      coordinates: COORDINATES,
      address_match_count: 1,
      geographies: GEOGRAPHIES,
    });
    const query = vi.fn().mockResolvedValue({ rows: [DISTRICT_ROW] });

    const result = await resolveAddressToDistricts(
      { query },
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA",
      { geocodeAddress, geocodeCoordinates, coordinates: COORDINATES }
    );

    expect(geocodeAddress).toHaveBeenCalledOnce();
    expect(result.districts).toHaveLength(1);
  });

  it("surfaces the coordinate error, not not_found, when both paths fail", async () => {
    const { CensusAddressGeocoderError } = await import("../../../src/pipeline/address/censusAddressGeocoder.js");
    const coordinateError = new CensusAddressGeocoderError("timeout", "coordinates lookup timed out");
    const geocodeCoordinates = vi.fn().mockRejectedValue(coordinateError);
    const geocodeAddress = vi
      .fn()
      .mockRejectedValue(new CensusAddressGeocoderError("not_found", "no street match"));
    const query = vi.fn();

    await expect(
      resolveAddressToDistricts({ query }, "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA", {
        geocodeAddress,
        geocodeCoordinates,
        coordinates: COORDINATES,
      })
    ).rejects.toBe(coordinateError);
    expect(geocodeAddress).toHaveBeenCalledOnce();
  });

  it("keeps not_found when the string path misses without a coordinate failure", async () => {
    const { CensusAddressGeocoderError } = await import("../../../src/pipeline/address/censusAddressGeocoder.js");
    const geocodeAddress = vi
      .fn()
      .mockRejectedValue(new CensusAddressGeocoderError("not_found", "no street match"));
    const query = vi.fn();

    await expect(
      resolveAddressToDistricts({ query }, "1 Nowhere Rd, Nowhere, NJ 07073", { geocodeAddress })
    ).rejects.toMatchObject({ code: "not_found" });
  });

  it("propagates district DB failures from the point path", async () => {
    const geocodeCoordinates = vi.fn().mockResolvedValue({ geographies: GEOGRAPHIES });
    const geocodeAddress = vi.fn();
    const query = vi.fn().mockRejectedValue(new Error("db down"));

    await expect(
      resolveAddressToDistricts({ query }, "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA", {
        geocodeAddress,
        geocodeCoordinates,
        coordinates: COORDINATES,
      })
    ).rejects.toThrow("db down");
    expect(geocodeAddress).not.toHaveBeenCalled();
  });
});
