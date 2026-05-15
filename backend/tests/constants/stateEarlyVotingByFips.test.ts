import { describe, expect, it } from "vitest";
import {
  getDeterministicEarlyVotingByFips,
  STATE_EARLY_VOTING_BY_FIPS,
} from "../../src/constants/stateEarlyVotingByFips.ts";

describe("stateEarlyVotingByFips", () => {
  it("covers 50 states + DC", () => {
    expect(Object.keys(STATE_EARLY_VOTING_BY_FIPS)).toHaveLength(51);
  });

  it("returns expected deterministic values for Arkansas", () => {
    expect(getDeterministicEarlyVotingByFips("05")).toEqual({
      available: true,
      start: "Fifteen days before election",
      end: "5 p.m. Monday before election",
    });
  });

  it("returns unavailable for states without early voting", () => {
    expect(getDeterministicEarlyVotingByFips("01")).toEqual({
      available: false,
      start: null,
      end: null,
    });
    expect(getDeterministicEarlyVotingByFips("28")).toEqual({
      available: false,
      start: null,
      end: null,
    });
    expect(getDeterministicEarlyVotingByFips("33")).toEqual({
      available: false,
      start: null,
      end: null,
    });
  });

  it("returns null for unknown fips", () => {
    expect(getDeterministicEarlyVotingByFips("99")).toBeNull();
  });
});
