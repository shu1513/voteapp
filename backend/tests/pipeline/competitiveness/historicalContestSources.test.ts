import { describe, expect, it } from "vitest";

import {
  listVerifiedHistoricalContestSourcePresets,
  VERIFIED_HISTORICAL_CONTEST_SOURCES,
  VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET,
} from "../../../src/pipeline/competitiveness/historicalContestSources.js";
import {
  type HistoricalContestOfficeType,
  HISTORICAL_CONTEST_OFFICE_TYPES,
} from "../../../src/pipeline/competitiveness/historicalContestKeys.js";

const CURRENTLY_IMPORTABLE_HISTORICAL_CONTEST_OFFICE_TYPES = [
  "US_PRESIDENT",
  "US_SENATE",
  "US_HOUSE",
  "GOVERNOR",
  "LIEUTENANT_GOVERNOR",
  "SECRETARY_OF_STATE",
  "ATTORNEY_GENERAL",
  "STATE_TREASURER",
  "STATE_AUDITOR",
  "COMPTROLLER",
  "SUPERINTENDENT_OF_PUBLIC_INSTRUCTION",
  "COMMISSIONER_OF_AGRICULTURE",
  "COMMISSIONER_OF_INSURANCE",
  "STATE_SENATE",
  "STATE_HOUSE",
] as const satisfies readonly HistoricalContestOfficeType[];

const STATEWIDE_EXECUTIVE_HISTORICAL_CONTEST_OFFICE_TYPES = [
  "LIEUTENANT_GOVERNOR",
  "SECRETARY_OF_STATE",
  "ATTORNEY_GENERAL",
  "STATE_TREASURER",
  "STATE_AUDITOR",
  "COMPTROLLER",
  "SUPERINTENDENT_OF_PUBLIC_INSTRUCTION",
  "COMMISSIONER_OF_AGRICULTURE",
  "COMMISSIONER_OF_INSURANCE",
] as const satisfies readonly HistoricalContestOfficeType[];

const STATE_OFFICE_SOURCE_PRESETS = [
  "medsl-2022-precinct",
  "medsl-2020-precinct-by-state",
  "medsl-2018-precinct-by-state",
  "medsl-2016-state-precinct",
  "medsl-2024-state-precinct",
] as const;

const FEDERAL_ONLY_SOURCE_PRESETS = [
  "medsl-2024-president-state",
  "medsl-2024-senate-state",
  "medsl-2024-house-precinct",
  "medsl-2016-president-precinct",
  "medsl-2016-senate-precinct",
  "medsl-2016-house-precinct",
] as const;

describe("historicalContestSources", () => {
  it("lists verified MEDSL source presets", () => {
    expect(listVerifiedHistoricalContestSourcePresets()).toEqual([
      "medsl-2024-president-state",
      "medsl-2024-senate-state",
      "medsl-2024-house-precinct",
      "medsl-2022-precinct",
      "medsl-2020-precinct-by-state",
      "medsl-2018-precinct-by-state",
      "medsl-2016-president-precinct",
      "medsl-2016-senate-precinct",
      "medsl-2016-house-precinct",
      "medsl-2016-state-precinct",
      "medsl-2024-state-precinct",
    ]);
  });

  it("keeps preset names unique and mapped to their source definitions", () => {
    const presets = VERIFIED_HISTORICAL_CONTEST_SOURCES.map((source) => source.preset);

    expect(new Set(presets).size).toBe(presets.length);
    for (const source of VERIFIED_HISTORICAL_CONTEST_SOURCES) {
      expect(VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET[source.preset]).toBe(source);
    }
  });

  it("uses HTTPS sources and supported office types only", () => {
    const supportedOfficeTypes = new Set(HISTORICAL_CONTEST_OFFICE_TYPES);

    for (const source of VERIFIED_HISTORICAL_CONTEST_SOURCES) {
      expect(source.sourceUrl.startsWith("https://")).toBe(true);
      for (const sourceFile of source.sourceFiles ?? []) {
        expect(sourceFile.startsWith("https://")).toBe(true);
      }
      if (source.sourceFiles) {
        expect(new Set(source.sourceFiles).size).toBe(source.sourceFiles.length);
      }
      for (const persistentId of source.sourceFileDiscovery?.dataverseDatasetPersistentIds ?? []) {
        expect(persistentId.startsWith("doi:10.7910/DVN/")).toBe(true);
      }
      if (source.sourceFileDiscovery?.dataverseDatasetPersistentIds) {
        const persistentIds = source.sourceFileDiscovery.dataverseDatasetPersistentIds;
        expect(new Set(persistentIds).size).toBe(persistentIds.length);
      }
      expect(source.electionYear).toBeGreaterThanOrEqual(1800);
      expect(source.electionYear).toBeLessThanOrEqual(2100);
      expect(source.officeTypes.length).toBeGreaterThan(0);
      for (const officeType of source.officeTypes) {
        expect(supportedOfficeTypes.has(officeType)).toBe(true);
      }
    }
  });

  it("covers every currently importable historical contest office type with a verified source", () => {
    const coveredOfficeTypes = new Set(
      VERIFIED_HISTORICAL_CONTEST_SOURCES.flatMap((source) => source.officeTypes)
    );
    const supportedOfficeTypes = new Set(HISTORICAL_CONTEST_OFFICE_TYPES);

    expect(coveredOfficeTypes).toEqual(new Set(CURRENTLY_IMPORTABLE_HISTORICAL_CONTEST_OFFICE_TYPES));
    for (const officeType of coveredOfficeTypes) {
      expect(supportedOfficeTypes.has(officeType)).toBe(true);
    }
  });

  it("enables safe statewide executive offices only on state-office precinct sources", () => {
    for (const preset of STATE_OFFICE_SOURCE_PRESETS) {
      const source = VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET[preset];
      expect(source.officeTypes).toEqual(
        expect.arrayContaining(STATEWIDE_EXECUTIVE_HISTORICAL_CONTEST_OFFICE_TYPES)
      );
    }

    for (const preset of FEDERAL_ONLY_SOURCE_PRESETS) {
      const source = VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET[preset];
      for (const officeType of STATEWIDE_EXECUTIVE_HISTORICAL_CONTEST_OFFICE_TYPES) {
        expect(source.officeTypes).not.toContain(officeType);
      }
    }
  });

  it("covers multiple election years for weighted historical margins", () => {
    expect(new Set(VERIFIED_HISTORICAL_CONTEST_SOURCES.map((source) => source.electionYear))).toEqual(
      new Set([2016, 2018, 2020, 2022, 2024])
    );

    const governorYears = VERIFIED_HISTORICAL_CONTEST_SOURCES.filter((source) =>
      source.officeTypes.includes("GOVERNOR")
    ).map((source) => source.electionYear);

    expect(new Set(governorYears)).toEqual(new Set([2016, 2018, 2020, 2022, 2024]));
  });

  it("uses per-state files for large precinct imports", () => {
    const sourceFileCounts = Object.fromEntries(
      VERIFIED_HISTORICAL_CONTEST_SOURCES.filter((source) => source.sourceFiles).map((source) => [
        source.preset,
        source.sourceFiles?.length,
      ])
    );

    expect(sourceFileCounts).toMatchObject({
      "medsl-2024-state-precinct": 51,
      "medsl-2022-precinct": 51,
    });
  });

  it("discovers guestbook-gated Dataverse files for older precinct imports", () => {
    const sourcesByPreset = VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET;

    expect(sourcesByPreset["medsl-2020-precinct-by-state"]).toMatchObject({
      source: "MIT_2020",
      sourceUrl: "https://doi.org/10.7910/DVN/NT66Z3",
      downloadMode: "dataverse_guestbook",
      sourceFileDiscovery: {
        dataverseDatasetPersistentIds: ["doi:10.7910/DVN/NT66Z3"],
      },
    });
    expect(sourcesByPreset["medsl-2018-precinct-by-state"]).toMatchObject({
      source: "MIT_2018",
      sourceUrl: "https://doi.org/10.7910/DVN/NVQYMG",
      downloadMode: "dataverse_guestbook",
      sourceFileDiscovery: {
        dataverseDatasetPersistentIds: ["doi:10.7910/DVN/NVQYMG"],
      },
    });
    expect(sourcesByPreset["medsl-2016-president-precinct"]).toMatchObject({
      source: "MIT_2016",
      sourceUrl: "https://doi.org/10.7910/DVN/LYWX3D",
      downloadMode: "dataverse_guestbook",
      sourceFileDiscovery: {
        dataverseDatasetPersistentIds: ["doi:10.7910/DVN/LYWX3D"],
      },
    });
    expect(sourcesByPreset["medsl-2016-state-precinct"]).toMatchObject({
      source: "MIT_2016",
      sourceUrl: "https://doi.org/10.7910/DVN/GSZG1O",
      downloadMode: "dataverse_guestbook",
      sourceFileDiscovery: {
        dataverseDatasetPersistentIds: ["doi:10.7910/DVN/GSZG1O"],
      },
    });
  });
});
