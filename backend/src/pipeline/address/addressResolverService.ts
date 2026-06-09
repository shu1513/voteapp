import type { Pool, PoolClient } from "pg";

import {
  buildAddressLookupCacheKey,
  DEFAULT_ADDRESS_LOOKUP_CACHE_TTL_SECONDS,
  readAddressLookupCache,
  type AddressLookupCacheClient,
  writeAddressLookupCache,
} from "./addressResolutionCache.js";
import {
  type CensusAddressGeocodeResult,
  type CensusAddressGeocoderOptions,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE,
  geocodeAddressWithCensus,
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
  geocoderOptions?: CensusAddressGeocoderOptions;
  cache?: AddressLookupCacheClient;
  cacheTtlSeconds?: number;
};

function effectiveGeocoderCacheContext(options: CensusAddressGeocoderOptions | undefined): {
  benchmark: string;
  vintage: string;
} {
  return {
    benchmark: options?.benchmark?.trim() || DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK,
    vintage: options?.vintage?.trim() || DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE,
  };
}

export async function resolveAddressToDistricts(
  db: Queryable,
  address: string,
  options: AddressResolverServiceOptions = {}
): Promise<AddressResolutionResult> {
  const geocodeAddress =
    options.geocodeAddress ?? ((input: string) => geocodeAddressWithCensus(input, options.geocoderOptions));
  const cacheContext = effectiveGeocoderCacheContext(options.geocoderOptions);
  const cacheKey = buildAddressLookupCacheKey({ address, ...cacheContext });
  const cached = options.cache
    ? await readAddressLookupCache(options.cache, cacheKey).catch(() => null)
    : null;

  const resolved = cached
    ? cached
    : await (async () => {
        const geocoded = await geocodeAddress(address);
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
