import { describe, expect, it } from "vitest";

import { parseUtahCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerUtahCandidateFinanceSync.js";

describe("triggerUtahCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseUtahCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/utah-cache",
        "--refresh-cache",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/utah-cache",
      refreshCache: true,
    });
  });

  it("rejects malformed integer flags", () => {
    expect(() => parseUtahCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUtahCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUtahCandidateFinanceSyncTriggerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });

});
