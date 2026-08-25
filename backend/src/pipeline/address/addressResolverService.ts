import type { Pool, PoolClient } from "pg";

import {
  buildAddressLookupCacheKey,
  DEFAULT_ADDRESS_LOOKUP_CACHE_TTL_SECONDS,
  readAddressLookupCache,
  type AddressLookupCacheClient,
  writeAddressLookupCache,
} from "./addressResolutionCache.js";
import {
  type CensusAddressCoordinates,
  type CensusAddressGeocodeResult,
  type CensusAddressGeocoderOptions,
  CensusAddressGeocoderError,
  type CensusCoordinatesGeocodeResult,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE,
  geocodeAddressWithCensusFallbacks,
  geocodeCoordinatesWithCensus,
} from "./censusAddressGeocoder.js";
import {
  type AddressDistrictKey,
  type AddressDistrictResolverWarning,
  resolveAddressDistrictKeysFromGeographies,
} from "./addressDistrictResolver.js";
import {
  type AddressDistrictLookupKey,
  type AddressResolvedDistrict,
  lookupAddressDistricts,
} from "./addressDistrictLookup.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type AddressResolutionResult = {
  matched_address: string;
  coordinates: {
    lat: number;
    lng: number;
  };
  address_match_count: number;
  district_keys: AddressDistrictKey[];
  districts: AddressResolvedDistrict[];
  missing_district_keys: AddressDistrictLookupKey[];
  warnings: AddressDistrictResolverWarning[];
};

export type AddressResolverServiceOptions = {
  geocodeAddress?: (address: string) => Promise<CensusAddressGeocodeResult>;
  geocodeCoordinates?: (coordinates: CensusAddressCoordinates) => Promise<CensusCoordinatesGeocodeResult>;
  geocoderOptions?: CensusAddressGeocoderOptions;
  cache?: AddressLookupCacheClient;
  cacheTtlSeconds?: number;
  /**
   * Client-supplied coordinates for the address (from the Google Places
   * autocomplete selection). When present the resolver looks districts up by
   * point instead of re-parsing the address string with the Census one-line
   * geocoder — venue-style addresses (stadiums, campuses) are routinely
   * absent from the Census street-range data even though Google locates
   * them. The address-string path stays as the fallback.
   */
  coordinates?: CensusAddressCoordinates;
};

function effectiveGeocoderCacheContext(options: CensusAddressGeocoderOptions | undefined): {
  benchmark: string;
  vintage: string;
  layers: string;
} {
  return {
    benchmark: options?.benchmark?.trim() || DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK,
    vintage: options?.vintage?.trim() || DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE,
    layers: options?.layers?.trim() || DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS,
  };
}

export async function resolveAddressToDistricts(
  db: Queryable,
  address: string,
  options: AddressResolverServiceOptions = {}
): Promise<AddressResolutionResult> {
  const geocodeAddress =
    options.geocodeAddress ?? ((input: string) => geocodeAddressWithCensusFallbacks(input, options.geocoderOptions));

  // Coordinate-first path. Never cached: the coordinates come from a Google
  // Places response, and Google's ToS forbids persisting Places data — the
  // redis cache would store them for 14 days. A dropped cache is acceptable
  // here; this path runs once per explicit dropdown selection.
  let coordinateGeocodeError: CensusAddressGeocoderError | null = null;
  if (options.coordinates) {
    const geocodeCoordinates =
      options.geocodeCoordinates ??
      ((input: CensusAddressCoordinates) => geocodeCoordinatesWithCensus(input, options.geocoderOptions));
    try {
      const located = await geocodeCoordinates(options.coordinates);
      const keyResolution = resolveAddressDistrictKeysFromGeographies(located.geographies);
      // Zero keys means the point matched no supported geography (bad
      // coordinates, or a Census data gap) — let the address-string path
      // below try, and fail with its clearer not_found if it also misses.
      if (keyResolution.district_keys.length > 0) {
        const districtLookup = await lookupAddressDistricts(db, keyResolution.district_keys);
        return {
          matched_address: address.trim(),
          coordinates: options.coordinates,
          address_match_count: 1,
          district_keys: keyResolution.district_keys,
          districts: districtLookup.districts,
          missing_district_keys: districtLookup.missing_district_keys,
          warnings: keyResolution.warnings,
        };
      }
    } catch (error) {
      // Geocoder trouble (timeout, 5xx, malformed body) falls back to the
      // address-string path; anything else (e.g. the district DB lookup) is
      // a real failure and propagates. The error is kept: if the fallback
      // ends in not_found — predictable for the venue addresses this path
      // exists for — the response must say "upstream trouble, retry", not
      // "check your address".
      if (!(error instanceof CensusAddressGeocoderError)) {
        throw error;
      }
      coordinateGeocodeError = error;
    }
  }

  const cacheContext = effectiveGeocoderCacheContext(options.geocoderOptions);
  const cacheKey = buildAddressLookupCacheKey({ address, ...cacheContext });
  const cached = options.cache
    ? await readAddressLookupCache(options.cache, cacheKey).catch(() => null)
    : null;

  const resolved = cached
    ? cached
    : await (async () => {
        const geocoded = await geocodeAddress(address).catch((error: unknown) => {
          // not_found after a failed coordinate lookup: the string parser
          // predictably misses venue addresses, so the honest error is the
          // coordinate path's retryable one, not "address not found".
          if (coordinateGeocodeError && error instanceof CensusAddressGeocoderError && error.code === "not_found") {
            throw coordinateGeocodeError;
          }
          throw error;
        });
        const keyResolution = resolveAddressDistrictKeysFromGeographies(geocoded.geographies);
        const value = {
          matched_address: geocoded.matched_address,
          coordinates: geocoded.coordinates,
          address_match_count: geocoded.address_match_count,
          district_keys: keyResolution.district_keys,
          warnings: keyResolution.warnings,
        };
        if (options.cache) {
          await writeAddressLookupCache(
            options.cache,
            cacheKey,
            value,
            options.cacheTtlSeconds ?? DEFAULT_ADDRESS_LOOKUP_CACHE_TTL_SECONDS
          ).catch(() => undefined);
        }
        return value;
      })();

  const districtLookup = await lookupAddressDistricts(db, resolved.district_keys);

  return {
    matched_address: resolved.matched_address,
    coordinates: resolved.coordinates,
    address_match_count: resolved.address_match_count,
    district_keys: resolved.district_keys,
    districts: districtLookup.districts,
    missing_district_keys: districtLookup.missing_district_keys,
    warnings: resolved.warnings,
  };
}
