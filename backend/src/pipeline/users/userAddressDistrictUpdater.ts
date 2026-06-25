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

export async function updateAuthenticatedAddressDistricts(
  dependencies: AuthenticatedAddressDistrictUpdaterDependencies,
  userId: string,
  address: string
): Promise<AuthenticatedAddressDistrictUpdateResult> {
  const resolved = await dependencies.resolveAddressToDistricts(address);
  const districtIds = resolved.districts.map((district) => district.id);
  if (districtIds.length === 0) {
    throw new TypeError("Resolved address did not match any supported districts");
  }

  await dependencies.replaceUserDistricts(userId, districtIds);
  const ballot = await dependencies.lookupBallotSummariesByDistrictIds(districtIds);
  return {
    matched_address: resolved.matched_address,
    ...ballot,
  };
}
