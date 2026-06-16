import { afterEach, describe, expect, it } from "vitest";

import {
  isPresidentialElectionsEnabled,
  isPresidentialFeatureEnabled,
} from "../../src/config/featureFlags.js";

const ORIGINAL_VALUE = process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
const ORIGINAL_ROSTER_VALUE = process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED;

describe("featureFlags", () => {
  afterEach(() => {
    if (ORIGINAL_VALUE === undefined) {
      delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
    } else {
      process.env.PRESIDENTIAL_ELECTIONS_ENABLED = ORIGINAL_VALUE;
    }
    if (ORIGINAL_ROSTER_VALUE === undefined) {
      delete process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED;
    } else {
      process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED = ORIGINAL_ROSTER_VALUE;
    }
  });

  it("enables presidential elections by default", () => {
    delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;

    expect(isPresidentialElectionsEnabled()).toBe(true);
  });

  it("reads enabled boolean values", () => {
    for (const value of ["true", "1", "yes", "y", "on", " TRUE "]) {
      process.env.PRESIDENTIAL_ELECTIONS_ENABLED = value;

      expect(isPresidentialElectionsEnabled()).toBe(true);
    }
  });

  it("reads disabled boolean values", () => {
    for (const value of ["false", "0", "no", "n", "off", " FALSE "]) {
      process.env.PRESIDENTIAL_ELECTIONS_ENABLED = value;

      expect(isPresidentialElectionsEnabled()).toBe(false);
    }
  });

  it("treats blank values as the default", () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "   ";

    expect(isPresidentialElectionsEnabled()).toBe(true);
  });

  it("rejects invalid boolean values", () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "maybe";

    expect(() => isPresidentialElectionsEnabled()).toThrow(
      "Invalid boolean env PRESIDENTIAL_ELECTIONS_ENABLED: maybe"
    );
  });

  it("requires the master presidential flag before a specific feature can run", () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED = "true";

    expect(isPresidentialFeatureEnabled("PRESIDENTIAL_ROSTER_RESEARCH_ENABLED")).toBe(false);
    expect(isPresidentialFeatureEnabled("PRESIDENTIAL_ROSTER_RESEARCH_ENABLED", true)).toBe(false);
  });

  it("allows force to bypass only the specific feature flag", () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "true";
    process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED = "false";

    expect(isPresidentialFeatureEnabled("PRESIDENTIAL_ROSTER_RESEARCH_ENABLED")).toBe(false);
    expect(isPresidentialFeatureEnabled("PRESIDENTIAL_ROSTER_RESEARCH_ENABLED", true)).toBe(true);
  });
});
