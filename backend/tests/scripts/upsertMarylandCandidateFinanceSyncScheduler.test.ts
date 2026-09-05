import { describe, expect, it } from "vitest";

import { parseUpsertMarylandCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertMarylandCandidateFinanceSyncScheduler.js";

describe("upsertMarylandCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler payload options", () => {
    expect(
      parseUpsertMarylandCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=20",
        "--stale-after-days=4",
        "--lookback-days",
        "30",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/maryland-cfs",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 20,
      staleAfterDays: 4,
      electionLookbackDays: 30,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/maryland-cfs",
    });
  });

  it("rejects malformed scheduler payload options", () => {
    expect(() => parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--max-candidates=10", "--max-candidates", "20"])).toThrow(
      "Provide --max-candidates at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });
});
