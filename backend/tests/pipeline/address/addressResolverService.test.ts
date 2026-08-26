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
      scope: "exact",
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

describe("resolveAddressToDistricts ZIP partial path", () => {
  const STATEWIDE_ROW = {
    id: "district-tx",
    district_type: "statewide",
    geoid_compact: "48",
    name: "Texas",
    state: "TX",
    state_fips: "48",
    population: 29145505,
    representation_power_score: null,
    requested_district_type: "statewide",
    requested_geoid_compact: "48",
  };
  const COUNTY_ROW = {
    id: "district-travis",
    district_type: "county",
    geoid_compact: "48453",
    name: "Travis County",
    state: "TX",
    state_fips: "48",
    population: 1290188,
    representation_power_score: "8.10",
    requested_district_type: "county",
    requested_geoid_compact: "48453",
  };

  // First query = crosswalk lookup, second = district lookup.
  function dbReturning(countyGeoids: string[], districtRows: unknown[]) {
    return vi
      .fn()
      .mockResolvedValueOnce({ rows: countyGeoids.map((county_geoid) => ({ county_geoid })) })
      .mockResolvedValueOnce({ rows: districtRows });
  }

  it("rejects ZIP-shaped input with full_address_required unless allowPartial is set", async () => {
    const geocodeAddress = vi.fn();
    const query = vi.fn();

    for (const input of ["78701", " 78701 ", "78701-2401"]) {
      await expect(
        resolveAddressToDistricts({ query }, input, { geocodeAddress })
      ).rejects.toMatchObject({ name: "ZipDistrictResolutionError", code: "full_address_required" });
    }
    // Never reaches the geocoder or the database: the refusal must not
    // depend on network or crosswalk state.
    expect(geocodeAddress).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalled();
  });

  it("resolves a single-county ZIP to statewide + county with zip scope and no coordinates", async () => {
    const geocodeAddress = vi.fn();
    const query = dbReturning(["48453"], [STATEWIDE_ROW, COUNTY_ROW]);

    const result = await resolveAddressToDistricts({ query }, "78701", { geocodeAddress, allowPartial: true });

    expect(geocodeAddress).not.toHaveBeenCalled();
    expect(query.mock.calls[0]?.[1]).toEqual(["78701"]);
    expect(query.mock.calls[1]?.[1]).toEqual([
      ["statewide", "county"],
      ["48", "48453"],
    ]);
    expect(result).toMatchObject({
      matched_address: "78701",
      coordinates: null,
      scope: "zip",
      address_match_count: 1,
      missing_district_keys: [],
      warnings: [],
    });
    expect(result.districts.map((district) => district.id)).toEqual(["district-tx", "district-travis"]);
  });

  it("trims ZIP+4 to the five-digit ZCTA", async () => {
    const query = dbReturning(["48453"], [STATEWIDE_ROW, COUNTY_ROW]);

    const result = await resolveAddressToDistricts({ query }, "78701-2401", { allowPartial: true });

    expect(query.mock.calls[0]?.[1]).toEqual(["78701"]);
    expect(result.matched_address).toBe("78701");
  });

  it("returns statewide only for a same-state multi-county ZIP", async () => {
    const query = dbReturning(["48453", "48491"], [STATEWIDE_ROW]);

    const result = await resolveAddressToDistricts({ query }, "78660", { allowPartial: true });

    // No county key at all — the visitor may live in either county.
    expect(query.mock.calls[1]?.[1]).toEqual([["statewide"], ["48"]]);
    expect(result.scope).toBe("zip");
    expect(result.districts.map((district) => district.id)).toEqual(["district-tx"]);
  });

  it("rejects a ZIP crossing state lines, even with a dominant county", async () => {
    // 02861 is 99.5% Providence County RI by land but intersects Bristol
    // County MA: land dominance is not address dominance, so refuse.
    const query = dbReturning(["25005", "44007"], []);

    await expect(resolveAddressToDistricts({ query }, "02861", { allowPartial: true })).rejects.toMatchObject({
      code: "zip_multi_state",
    });
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("rejects a ZIP with no crosswalk rows as zip_not_found", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });

    await expect(resolveAddressToDistricts({ query }, "00001", { allowPartial: true })).rejects.toMatchObject({
      code: "zip_not_found",
    });
  });

  it("rejects territory ZIPs as zip_unsupported_region", async () => {
    // Puerto Rico: real ZCTA, but the districts table covers 50 states + DC.
    const query = dbReturning(["72127"], []);

    await expect(resolveAddressToDistricts({ query }, "00901", { allowPartial: true })).rejects.toMatchObject({
      code: "zip_unsupported_region",
    });
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("leaves non-ZIP input on the exact pipeline even with allowPartial", async () => {
    const geocodeAddress = vi.fn().mockResolvedValue({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      coordinates: { lat: 34.08, lng: -117.98 },
      address_match_count: 1,
      geographies: {},
    });
    const query = vi.fn().mockResolvedValue({ rows: [] });

    const result = await resolveAddressToDistricts({ query }, "Austin, TX 78701, USA", {
      geocodeAddress,
      allowPartial: true,
    });

    expect(geocodeAddress).toHaveBeenCalledWith("Austin, TX 78701, USA");
    expect(result.scope).toBe("exact");
  });
});

describe("resolveAddressToDistricts region partial path", () => {
  const STATEWIDE_ROW = {
    id: "district-ca",
    district_type: "statewide",
    geoid_compact: "06",
    name: "California",
    state: "CA",
    state_fips: "06",
    population: 39538223,
    representation_power_score: null,
    requested_district_type: "statewide",
    requested_geoid_compact: "06",
  };
  const PLACE_ROW = {
    id: "district-la",
    district_type: "place",
    geoid_compact: "0644000",
    name: "Los Angeles city, California",
    state: "CA",
    state_fips: "06",
    population: 3898747,
    representation_power_score: "7.55",
    requested_district_type: "place",
    requested_geoid_compact: "0644000",
  };

  it("rejects a region selection with full_address_required unless allowPartial is set", async () => {
    const geocodeAddress = vi.fn();
    const query = vi.fn();

    await expect(
      resolveAddressToDistricts({ query }, "Los Angeles, CA, USA", { geocodeAddress, regionState: "CA" })
    ).rejects.toMatchObject({ name: "ZipDistrictResolutionError", code: "full_address_required" });
    expect(geocodeAddress).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalled();
  });

  it("rejects a state outside the covered 50 + DC as region_unsupported", async () => {
    const query = vi.fn();

    await expect(
      resolveAddressToDistricts({ query }, "San Juan, PR, USA", { allowPartial: true, regionState: "PR" })
    ).rejects.toMatchObject({ code: "region_unsupported" });
    expect(query).not.toHaveBeenCalled();
  });

  it("resolves a stateful region with no locality to statewide only, region scope, no coordinates", async () => {
    const geocodeAddress = vi.fn();
    const query = vi.fn().mockResolvedValueOnce({ rows: [STATEWIDE_ROW] });

    const result = await resolveAddressToDistricts({ query }, "Los Angeles County, CA, USA", {
      geocodeAddress,
      allowPartial: true,
      regionState: "CA",
    });

    expect(geocodeAddress).not.toHaveBeenCalled();
    // Straight to the district lookup: no locality, no place-name query.
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[1]).toEqual([["statewide"], ["06"]]);
    expect(result).toMatchObject({
      matched_address: "Los Angeles County, CA, USA",
      coordinates: null,
      scope: "region",
      address_match_count: 1,
      missing_district_keys: [],
      warnings: [],
    });
    expect(result.districts.map((district) => district.id)).toEqual(["district-ca"]);
  });

  it("adds the place when the locality matches exactly one incorporated place", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ geoid_compact: "0644000" }] })
      .mockResolvedValueOnce({ rows: [STATEWIDE_ROW, PLACE_ROW] });

    const result = await resolveAddressToDistricts({ query }, "Los Angeles, CA, USA", {
      allowPartial: true,
      regionState: "CA",
      regionLocality: "Los Angeles",
    });

    // Name candidates carry every incorporated legal-type suffix, lowercased,
    // and never the CDP form.
    expect(query.mock.calls[0]?.[1]).toEqual([
      "CA",
      [
        "los angeles city, california",
        "los angeles town, california",
        "los angeles village, california",
        "los angeles borough, california",
        "los angeles municipality, california",
      ],
    ]);
    expect(query.mock.calls[1]?.[1]).toEqual([
      ["statewide", "place"],
      ["06", "0644000"],
    ]);
    expect(result.scope).toBe("region");
    expect(result.districts.map((district) => district.id)).toEqual(["district-ca", "district-la"]);
  });

  it("stays statewide only when the locality matches zero or several places", async () => {
    for (const placeRows of [[], [{ geoid_compact: "0644000" }, { geoid_compact: "0644999" }]]) {
      const query = vi
        .fn()
        .mockResolvedValueOnce({ rows: placeRows })
        .mockResolvedValueOnce({ rows: [STATEWIDE_ROW] });

      const result = await resolveAddressToDistricts({ query }, "East Los Angeles, CA, USA", {
        allowPartial: true,
        regionState: "CA",
        regionLocality: "East Los Angeles",
      });

      expect(query.mock.calls[1]?.[1]).toEqual([["statewide"], ["06"]]);
      expect(result.districts.map((district) => district.id)).toEqual(["district-ca"]);
    }
  });

  it("lets a ZIP-shaped address win over a stray region_state", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ county_geoid: "06037" }] })
      .mockResolvedValueOnce({ rows: [STATEWIDE_ROW] });

    const result = await resolveAddressToDistricts({ query }, "91706", {
      allowPartial: true,
      regionState: "CA",
    });

    // First query is the ZCTA crosswalk, not the place-name lookup.
    expect(query.mock.calls[0]?.[1]).toEqual(["91706"]);
    expect(result.scope).toBe("zip");
  });
});
