import { describe, expect, it, vi } from "vitest";

import {
  AuthenticatedAddressDistrictUpdateError,
  updateAuthenticatedAddressDistricts,
} from "../../../src/pipeline/users/userAddressDistrictUpdater.js";
import type { AddressResolutionResult } from "../../../src/pipeline/address/addressResolverService.js";

const userId = "11111111-1111-4111-8111-111111111111";
const districtId = "22222222-2222-4222-8222-222222222222";

const resolvedAddress: AddressResolutionResult = {
  matched_address: "123 MAIN ST, DENVER, CO, 80203",
  coordinates: { lat: 39.7392, lng: -104.9903 },
  address_match_count: 1,
  district_keys: [],
  districts: [
    {
      id: districtId,
      district_type: "county",
      geoid_compact: "08031",
      name: "Denver County",
      state: "CO",
      state_fips: "08",
      population: 715522,
      representation_power_score: 50.4,
    },
  ],
  missing_district_keys: [],
  warnings: [],
};

describe("updateAuthenticatedAddressDistricts", () => {
  it("resolves an address, replaces saved districts, and returns the updated ballot", async () => {
    const resolveAddressToDistricts = vi.fn().mockResolvedValue(resolvedAddress);
    const replaceUserDistricts = vi.fn().mockResolvedValue({ districtCount: 1 });
    const lookupBallotSummariesByDistrictIds = vi.fn().mockResolvedValue({
      district_ids: [districtId],
      districts: resolvedAddress.districts,
      elections: [],
    });

    const result = await updateAuthenticatedAddressDistricts(
      { resolveAddressToDistricts, replaceUserDistricts, lookupBallotSummariesByDistrictIds },
      userId,
      "123 Main St Denver CO 80203"
    );

    expect(result).toEqual({
      matched_address: resolvedAddress.matched_address,
      district_ids: [districtId],
      districts: resolvedAddress.districts,
      elections: [],
    });
    expect(resolveAddressToDistricts).toHaveBeenCalledWith("123 Main St Denver CO 80203");
    expect(replaceUserDistricts).toHaveBeenCalledWith(userId, [districtId]);
    expect(lookupBallotSummariesByDistrictIds).toHaveBeenCalledWith([districtId]);
  });

  it("does not replace saved districts when the resolved address has no supported districts", async () => {
    const resolveAddressToDistricts = vi.fn().mockResolvedValue({ ...resolvedAddress, districts: [] });
    const replaceUserDistricts = vi.fn();
    const lookupBallotSummariesByDistrictIds = vi.fn();

    await expect(
      updateAuthenticatedAddressDistricts(
        { resolveAddressToDistricts, replaceUserDistricts, lookupBallotSummariesByDistrictIds },
        userId,
        "123 Main St Denver CO 80203"
      )
    ).rejects.toSatisfy((error) => {
      expect(error).toBeInstanceOf(AuthenticatedAddressDistrictUpdateError);
      expect((error as AuthenticatedAddressDistrictUpdateError).code).toBe("no_supported_districts");
      expect((error as Error).message).toBe("Resolved address did not match any supported districts");
      return true;
    });

    expect(replaceUserDistricts).not.toHaveBeenCalled();
    expect(lookupBallotSummariesByDistrictIds).not.toHaveBeenCalled();
  });
});
