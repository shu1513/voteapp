import { describe, expect, it } from "vitest";

import { parseUpsertMichiganCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertMichiganCandidateFinanceSyncScheduler.js";

describe("upsertMichiganCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertMichiganCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-extracted-dir=/tmp/2022_mi_cfr",
        "--raw-cache-dir=/tmp/michigan-cache",
        "--ai-min-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataExtractedDir: "/tmp/2022_mi_cfr",
      rawDataCacheDir: "/tmp/michigan-cache",
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("rejects malformed integer and path flags", () => {
    expect(() => parseUpsertMichiganCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertMichiganCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseUpsertMichiganCandidateFinanceSyncSchedulerArgs(["--raw-extracted-dir=   "])).toThrow(
      "Missing --raw-extracted-dir value"
    );
    expect(() => parseUpsertMichiganCandidateFinanceSyncSchedulerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });
});
