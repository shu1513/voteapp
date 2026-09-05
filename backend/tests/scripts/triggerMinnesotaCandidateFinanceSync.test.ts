import { describe, expect, it } from "vitest";

import { parseMinnesotaCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerMinnesotaCandidateFinanceSync.js";

describe("triggerMinnesotaCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseMinnesotaCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/mn",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/mn",
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseMinnesotaCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseMinnesotaCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseMinnesotaCandidateFinanceSyncTriggerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseMinnesotaCandidateFinanceSyncTriggerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });
});
