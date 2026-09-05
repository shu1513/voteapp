import { describe, expect, it } from "vitest";

import { parseUpsertColoradoCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertColoradoCandidateFinanceSyncScheduler.js";

describe("upsertColoradoCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertColoradoCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-zip=/tmp/2026_ContributionData.csv.zip",
        "--raw-cache-dir=/tmp/colorado-cache",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataZipPath: "/tmp/2026_ContributionData.csv.zip",
      rawDataCacheDir: "/tmp/colorado-cache",
    });
  });

  it("rejects malformed integer flags", () => {
    expect(() => parseUpsertColoradoCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertColoradoCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseUpsertColoradoCandidateFinanceSyncSchedulerArgs(["--max-candidates=10", "--max-candidates", "20"])).toThrow(
      "Provide --max-candidates at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertColoradoCandidateFinanceSyncSchedulerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });

  it("rejects whitespace-only path flags", () => {
    expect(() => parseUpsertColoradoCandidateFinanceSyncSchedulerArgs(["--raw-zip=   "])).toThrow(
      "Missing --raw-zip value"
    );
    expect(() => parseUpsertColoradoCandidateFinanceSyncSchedulerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });
});
