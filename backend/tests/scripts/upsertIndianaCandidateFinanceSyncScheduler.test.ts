import { describe, expect, it } from "vitest";

import { parseUpsertIndianaCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertIndianaCandidateFinanceSyncScheduler.js";

describe("upsertIndianaCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertIndianaCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/indiana",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/indiana",
    });
  });

  it("rejects malformed, missing, and duplicate value flags", () => {
    expect(() => parseUpsertIndianaCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertIndianaCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() =>
      parseUpsertIndianaCandidateFinanceSyncSchedulerArgs(["--raw-cache-dir=/tmp/a", "--raw-cache-dir=/tmp/b"])
    ).toThrow("Provide --raw-cache-dir at most once");
    expect(() => parseUpsertIndianaCandidateFinanceSyncSchedulerArgs(["--bogus"])).toThrow(
      "Unknown Indiana campaign finance flag: --bogus"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertIndianaCandidateFinanceSyncSchedulerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });
});
