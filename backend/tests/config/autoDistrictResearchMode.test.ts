import { afterEach, describe, expect, it } from "vitest";

import { readAutoDistrictResearchMode } from "../../src/config/featureFlags.js";

const ORIGINAL_MODE = process.env.AUTO_DISTRICT_RESEARCH_MODE;
const ORIGINAL_LEGACY = process.env.AUTO_DISTRICT_RESEARCH_ENABLED;

afterEach(() => {
  if (ORIGINAL_MODE === undefined) {
    delete process.env.AUTO_DISTRICT_RESEARCH_MODE;
  } else {
    process.env.AUTO_DISTRICT_RESEARCH_MODE = ORIGINAL_MODE;
  }
  if (ORIGINAL_LEGACY === undefined) {
    delete process.env.AUTO_DISTRICT_RESEARCH_ENABLED;
  } else {
    process.env.AUTO_DISTRICT_RESEARCH_ENABLED = ORIGINAL_LEGACY;
  }
});

describe("readAutoDistrictResearchMode", () => {
  it("defaults to manual when neither env is set (feature on by default)", () => {
    delete process.env.AUTO_DISTRICT_RESEARCH_MODE;
    delete process.env.AUTO_DISTRICT_RESEARCH_ENABLED;

    expect(readAutoDistrictResearchMode()).toBe("manual");
  });

  it("turns fully off with an explicit off mode", () => {
    delete process.env.AUTO_DISTRICT_RESEARCH_ENABLED;
    process.env.AUTO_DISTRICT_RESEARCH_MODE = "off";

    expect(readAutoDistrictResearchMode()).toBe("off");
  });

  it("reads each explicit mode", () => {
    delete process.env.AUTO_DISTRICT_RESEARCH_ENABLED;
    for (const mode of ["off", "ai", "manual"] as const) {
      process.env.AUTO_DISTRICT_RESEARCH_MODE = mode;
      expect(readAutoDistrictResearchMode()).toBe(mode);
    }
  });

  it("normalizes case and whitespace", () => {
    delete process.env.AUTO_DISTRICT_RESEARCH_ENABLED;
    process.env.AUTO_DISTRICT_RESEARCH_MODE = "  Manual ";

    expect(readAutoDistrictResearchMode()).toBe("manual");
  });

  it("maps the legacy boolean to ai when the mode env is unset", () => {
    delete process.env.AUTO_DISTRICT_RESEARCH_MODE;
    process.env.AUTO_DISTRICT_RESEARCH_ENABLED = "true";

    expect(readAutoDistrictResearchMode()).toBe("ai");
  });

  it("accepts mode=ai alongside the legacy boolean", () => {
    process.env.AUTO_DISTRICT_RESEARCH_MODE = "ai";
    process.env.AUTO_DISTRICT_RESEARCH_ENABLED = "true";

    expect(readAutoDistrictResearchMode()).toBe("ai");
  });

  it("rejects a conflicting mode + legacy boolean combination", () => {
    process.env.AUTO_DISTRICT_RESEARCH_MODE = "manual";
    process.env.AUTO_DISTRICT_RESEARCH_ENABLED = "true";

    expect(() => readAutoDistrictResearchMode()).toThrow(/Conflicting config/);
  });

  it("rejects an unknown mode value", () => {
    delete process.env.AUTO_DISTRICT_RESEARCH_ENABLED;
    process.env.AUTO_DISTRICT_RESEARCH_MODE = "hybrid";

    expect(() => readAutoDistrictResearchMode()).toThrow(/Invalid AUTO_DISTRICT_RESEARCH_MODE/);
  });
});
