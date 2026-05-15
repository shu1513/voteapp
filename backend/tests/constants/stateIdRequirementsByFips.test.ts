import { describe, expect, it } from "vitest";
import {
  getDeterministicIdRequirementByFips,
  STATE_ID_REQUIREMENTS_BY_FIPS,
} from "../../src/constants/stateIdRequirementsByFips.ts";

describe("stateIdRequirementsByFips", () => {
  it("covers 50 states + DC", () => {
    expect(Object.keys(STATE_ID_REQUIREMENTS_BY_FIPS)).toHaveLength(51);
  });

  it("returns deterministic values for known states", () => {
    expect(getDeterministicIdRequirementByFips("05")).toBe("Strict photo ID");
    expect(getDeterministicIdRequirementByFips("06")).toBe("Non-strict, non-photo ID");
  });

  it("returns null for unknown fips", () => {
    expect(getDeterministicIdRequirementByFips("99")).toBeNull();
  });
});
