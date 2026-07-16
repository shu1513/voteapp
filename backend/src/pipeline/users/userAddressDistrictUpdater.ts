import type { BallotSummaryResult } from "../address/ballotLookup.js";
import type { AddressResolutionResult } from "../address/addressResolverService.js";
import type { ReplaceUserDistrictsResult } from "./userDistrictReplacer.js";

export type AuthenticatedAddressDistrictUpdateResult = BallotSummaryResult & {
  matched_address: string;
};

export type AuthenticatedAddressDistrictUpdaterDependencies = {
  resolveAddressToDistricts: (address: string) => Promise<AddressResolutionResult>;
  replaceUserDistricts: (userId: string, districtIds: readonly string[]) => Promise<ReplaceUserDistrictsResult>;
  lookupBallotSummariesByDistrictIds: (districtIds: readonly string[]) => Promise<BallotSummaryResult>;
};

export type AuthenticatedAddressDistrictUpdateErrorCode = "no_supported_districts" | "partial_district_resolution";

export class AuthenticatedAddressDistrictUpdateError extends Error {
  constructor(
    readonly code: AuthenticatedAddressDistrictUpdateErrorCode,
    message: string
  ) {
    super(message);
    this.name = "AuthenticatedAddressDistrictUpdateError";
  }
}

export async function updateAuthenticatedAddressDistricts(
  dependencies: AuthenticatedAddressDistrictUpdaterDependencies,
  userId: string,
  address: string
): Promise<AuthenticatedAddressDistrictUpdateResult> {
  const resolved = await dependencies.resolveAddressToDistricts(address);

  // A key the Census geocoder returned but the districts table lacks
  // (vintage drift after redistricting, an incomplete load) means the
  // resolution is incomplete. Replacing the saved set with the partial
  // subset would silently drop valid districts — shrinking the user's
  // ballot and notifications — so refuse and leave the saved districts
  // untouched. The address API already logs the missing keys for the
  // operator, and the update succeeds once the districts data is reloaded.
  // Checked before the empty case: all keys missing is still a data gap,
  // not an unsupported address.
  if (resolved.missing_district_keys.length > 0) {
    throw new AuthenticatedAddressDistrictUpdateError(
      "partial_district_resolution",
      "Some districts for this address are temporarily unavailable, so your saved districts were left unchanged. Try again later."
    );
  }

  const districtIds = resolved.districts.map((district) => district.id);
  if (districtIds.length === 0) {
    throw new AuthenticatedAddressDistrictUpdateError(
      "no_supported_districts",
      "Resolved address did not match any supported districts"
    );
  }

  await dependencies.replaceUserDistricts(userId, districtIds);
  const ballot = await dependencies.lookupBallotSummariesByDistrictIds(districtIds);
  return {
    matched_address: resolved.matched_address,
    ...ballot,
  };
}
