import { describe, expect, it } from "vitest";

import { parseUpsertPennsylvaniaCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertPennsylvaniaCandidateFinanceSyncScheduler.js";

describe("upsertPennsylvaniaCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertPennsylvaniaCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-extracted-dir=/tmp/pa-cf/2022",
        "--raw-cache-dir=/tmp/pa-cf",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataExtractedDir: "/tmp/pa-cf/2022",
      rawDataCacheDir: "/tmp/pa-cf",
    });
  });

  it("rejects malformed integer and path flags", () => {
    expect(() => parseUpsertPennsylvaniaCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertPennsylvaniaCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseUpsertPennsylvaniaCandidateFinanceSyncSchedulerArgs(["--raw-extracted-dir=   "])).toThrow(
      "Missing --raw-extracted-dir value"
    );
    expect(() => parseUpsertPennsylvaniaCandidateFinanceSyncSchedulerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });
});
