import { describe, expect, it } from "vitest";

import { parseTexasCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerTexasCandidateFinanceSync.js";

describe("triggerTexasCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseTexasCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-zip=/tmp/2026_ContributionData.csv.zip",
        "--raw-cache-dir=/tmp/texas-cache",
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
    });
  });

  it("rejects malformed integer flags", () => {
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["--raw-zip="])).toThrow("Missing --raw-zip value");
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["--max-candidates=10", "--max-candidates", "20"])).toThrow(
      "Provide --max-candidates at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });

});
