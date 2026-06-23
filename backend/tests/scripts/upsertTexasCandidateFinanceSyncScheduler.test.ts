import { describe, expect, it } from "vitest";

import { parseUpsertTexasCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertTexasCandidateFinanceSyncScheduler.js";

describe("upsertTexasCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertTexasCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-zip=/tmp/2026_ContributionData.csv.zip",
        "--raw-cache-dir=/tmp/texas-cache",
        "--ai-classify-industries",
        "--ai-min-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataZipPath: "/tmp/2026_ContributionData.csv.zip",
      rawDataCacheDir: "/tmp/texas-cache",
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("rejects malformed integer flags", () => {
    expect(() => parseUpsertTexasCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertTexasCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

  it("rejects whitespace-only path flags", () => {
    expect(() => parseUpsertTexasCandidateFinanceSyncSchedulerArgs(["--raw-zip=   "])).toThrow(
      "Missing --raw-zip value"
    );
    expect(() => parseUpsertTexasCandidateFinanceSyncSchedulerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });
});
