import { describe, expect, it } from "vitest";

import { parseMarylandCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerMarylandCandidateFinanceSync.js";

describe("triggerMarylandCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseMarylandCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/maryland-cfs",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/maryland-cfs",
    });
  });

  it("rejects malformed integer flags", () => {
    expect(() => parseMarylandCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseMarylandCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseMarylandCandidateFinanceSyncTriggerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });
});
