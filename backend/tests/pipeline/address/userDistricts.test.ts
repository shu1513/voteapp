import { describe, expect, it, vi } from "vitest";

import { saveUserDistricts } from "../../../src/pipeline/address/userDistricts.js";
import type { AddressResolvedDistrict } from "../../../src/pipeline/address/addressDistrictLookup.js";

const userId = "11111111-1111-4111-8111-111111111111";
const countyDistrict: AddressResolvedDistrict = {
  id: "22222222-2222-4222-8222-222222222222",
  district_type: "county",
  geoid_compact: "06037",
  name: "Los Angeles County",
  state: "CA",
  state_fips: "06",
  population: 9876482,
  vote_power_score: 12.3,
};
const houseDistrict: AddressResolvedDistrict = {
  id: "33333333-3333-4333-8333-333333333333",
  district_type: "us_house",
  geoid_compact: "0631",
  name: "Congressional District 31",
  state: "CA",
  state_fips: "06",
  population: 760000,
  vote_power_score: null,
};

describe("saveUserDistricts", () => {
  it("returns zero without querying when there are no districts", async () => {
    const db = { query: vi.fn() };

    await expect(saveUserDistricts(db, userId, [])).resolves.toEqual({
      user_id: userId,
      district_count: 0,
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("upserts unique user districts", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 2 });

    await expect(saveUserDistricts({ query }, userId, [countyDistrict, houseDistrict, countyDistrict])).resolves.toEqual({
      user_id: userId,
      district_count: 2,
    });

    expect(query).toHaveBeenCalledOnce();
    expect(String(query.mock.calls[0]?.[0])).toContain("INSERT INTO public.user_districts");
    expect(query.mock.calls[0]?.[1]).toEqual([
      userId,
      [countyDistrict.id, houseDistrict.id],
      [countyDistrict.district_type, houseDistrict.district_type],
    ]);
  });

  it("rejects blank user IDs", async () => {
    const db = { query: vi.fn() };

    await expect(saveUserDistricts(db, "   ", [countyDistrict])).rejects.toThrow("userId must not be empty");
    expect(db.query).not.toHaveBeenCalled();
  });
});
