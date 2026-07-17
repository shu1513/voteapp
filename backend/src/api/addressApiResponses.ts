import type { AddressResolutionResult } from "../pipeline/address/addressResolverService.js";

export type PublicAddressResolutionResult = {
  matched_address: string;
  // How many candidate addresses the geocoder returned. The resolver uses the
  // first match, so anything above 1 means the input was ambiguous (e.g.
  // "100 Main St, Springfield" matches several states) and the client should
  // ask the user to confirm the matched address.
  address_match_count: number;
  districts: AddressResolutionResult["districts"];
};

export type AddressResolutionDiagnostics = {
  address_match_count: number;
  district_keys: AddressResolutionResult["district_keys"];
  missing_district_keys: AddressResolutionResult["missing_district_keys"];
  warnings: AddressResolutionResult["warnings"];
};

export function toPublicAddressResolution(result: AddressResolutionResult): PublicAddressResolutionResult {
  return {
    matched_address: result.matched_address,
    address_match_count: result.address_match_count,
    districts: result.districts,
  };
}

export function toAddressResolutionDiagnostics(result: AddressResolutionResult): AddressResolutionDiagnostics {
  return {
    address_match_count: result.address_match_count,
    district_keys: result.district_keys,
    missing_district_keys: result.missing_district_keys,
    warnings: result.warnings,
  };
}
