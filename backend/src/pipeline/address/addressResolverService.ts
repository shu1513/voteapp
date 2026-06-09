import type { Pool, PoolClient } from "pg";

import {
  type CensusAddressGeocodeResult,
  type CensusAddressGeocoderOptions,
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
};

export async function resolveAddressToDistricts(
  db: Queryable,
  address: string,
  options: AddressResolverServiceOptions = {}
): Promise<AddressResolutionResult> {
  const geocodeAddress =
    options.geocodeAddress ?? ((input: string) => geocodeAddressWithCensus(input, options.geocoderOptions));
  const geocoded = await geocodeAddress(address);
  const keyResolution = resolveAddressDistrictKeysFromGeographies(geocoded.geographies);
  const districtLookup = await lookupAddressDistricts(db, keyResolution.district_keys);

  return {
    matched_address: geocoded.matched_address,
    coordinates: geocoded.coordinates,
    address_match_count: geocoded.address_match_count,
    district_keys: keyResolution.district_keys,
    districts: districtLookup.districts,
    missing_district_keys: districtLookup.missing_district_keys,
    warnings: keyResolution.warnings,
  };
}
