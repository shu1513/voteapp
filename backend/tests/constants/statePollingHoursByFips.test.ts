import { describe, expect, it } from "vitest";
import {
  getDeterministicPollingHoursByFips,
  STATE_POLLING_HOURS_BY_FIPS,
} from "../../src/constants/statePollingHoursByFips.ts";

describe("statePollingHoursByFips", () => {
  it("covers 50 states + DC", () => {
    expect(Object.keys(STATE_POLLING_HOURS_BY_FIPS)).toHaveLength(51);
  });

  it("returns deterministic values for known states", () => {
    expect(getDeterministicPollingHoursByFips("06")).toContain("7 a.m. to 8 p.m.");
    expect(getDeterministicPollingHoursByFips("11")).toContain("7 a.m. to 8 p.m.");
  });

  it("returns null for unknown fips", () => {
    expect(getDeterministicPollingHoursByFips("99")).toBeNull();
  });
});

