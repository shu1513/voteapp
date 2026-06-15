import { describe, expect, it } from "vitest";

import {
  listVerifiedHistoricalContestSourcePresets,
  VERIFIED_HISTORICAL_CONTEST_SOURCES,
  VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET,
} from "../../../src/pipeline/competitiveness/historicalContestSources.js";
import { HISTORICAL_CONTEST_OFFICE_TYPES } from "../../../src/pipeline/competitiveness/historicalContestKeys.js";

describe("historicalContestSources", () => {
  it("lists verified MEDSL source presets", () => {
    expect(listVerifiedHistoricalContestSourcePresets()).toEqual([
      "medsl-2024-president-state",
      "medsl-2024-senate-state",
    ]);
  });

  it("keeps preset names unique and mapped to their source definitions", () => {
    const presets = VERIFIED_HISTORICAL_CONTEST_SOURCES.map((source) => source.preset);

    expect(new Set(presets).size).toBe(presets.length);
    for (const source of VERIFIED_HISTORICAL_CONTEST_SOURCES) {
      expect(VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET[source.preset]).toBe(source);
    }
  });

  it("uses aggregate HTTPS CSV sources and supported office types only", () => {
    const supportedOfficeTypes = new Set(HISTORICAL_CONTEST_OFFICE_TYPES);

    for (const source of VERIFIED_HISTORICAL_CONTEST_SOURCES) {
      expect(source.sourceUrl.startsWith("https://")).toBe(true);
      expect(source.sourceUrl.endsWith(".csv")).toBe(true);
      expect(source.format).toBe("medsl_aggregate_csv");
      expect(source.electionYear).toBeGreaterThanOrEqual(1800);
      expect(source.electionYear).toBeLessThanOrEqual(2100);
      expect(source.officeTypes.length).toBeGreaterThan(0);
      for (const officeType of source.officeTypes) {
        expect(supportedOfficeTypes.has(officeType)).toBe(true);
      }
    }
  });
});
