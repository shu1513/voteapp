import { describe, expect, it } from "vitest";

import { parseVerifiedHistoricalContestMarginImportArgs } from "../../src/scripts/importVerifiedHistoricalContestMarginsCli.js";

describe("importVerifiedHistoricalContestMarginsCli", () => {
  it("defaults to a real import", () => {
    expect(parseVerifiedHistoricalContestMarginImportArgs([])).toEqual({
      dryRun: false,
      preset: null,
    });
  });

  it("parses dry-run imports", () => {
    expect(parseVerifiedHistoricalContestMarginImportArgs(["--dry-run"])).toEqual({
      dryRun: true,
      preset: null,
    });
  });

  it("parses a verified source preset filter", () => {
    expect(
      parseVerifiedHistoricalContestMarginImportArgs(["--preset=medsl-2022-precinct", "--dry-run"])
    ).toEqual({
      dryRun: true,
      preset: "medsl-2022-precinct",
    });
  });

  it("rejects duplicate preset filters", () => {
    expect(() =>
      parseVerifiedHistoricalContestMarginImportArgs([
        "--preset=medsl-2024-house-precinct",
        "--preset=medsl-2022-precinct",
      ])
    ).toThrow("Provide at most one verified historical contest import preset");
  });

  it("rejects unknown preset filters", () => {
    expect(() => parseVerifiedHistoricalContestMarginImportArgs(["--preset=missing"])).toThrow(
      "Unknown verified historical contest import preset: missing. Known presets:"
    );
  });

  it("rejects unknown arguments", () => {
    expect(() => parseVerifiedHistoricalContestMarginImportArgs(["--source=MIT_2024"])).toThrow(
      "Unknown verified historical contest import argument: --source=MIT_2024"
    );
  });
});
