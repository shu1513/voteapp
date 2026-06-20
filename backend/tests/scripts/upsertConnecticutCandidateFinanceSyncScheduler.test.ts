import { describe, expect, it } from "vitest";

import { parseUpsertConnecticutCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertConnecticutCandidateFinanceSyncScheduler.js";

describe("upsertConnecticutCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertConnecticutCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/ecris",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/ecris",
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseUpsertConnecticutCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertConnecticutCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseUpsertConnecticutCandidateFinanceSyncSchedulerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });
});
