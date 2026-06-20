import { describe, expect, it } from "vitest";

import { parseColoradoCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerColoradoCandidateFinanceSync.js";

describe("triggerColoradoCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseColoradoCandidateFinanceSyncTriggerArgs([
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
    expect(() => parseColoradoCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseColoradoCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });
});
