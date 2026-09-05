import { describe, expect, it } from "vitest";

import { parseIndianaCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerIndianaCandidateFinanceSync.js";

describe("triggerIndianaCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseIndianaCandidateFinanceSyncTriggerArgs([
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
    expect(() => parseIndianaCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseIndianaCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() =>
      parseIndianaCandidateFinanceSyncTriggerArgs(["--max-candidates=5", "--max-candidates", "6"])
    ).toThrow("Provide --max-candidates at most once");
    expect(() => parseIndianaCandidateFinanceSyncTriggerArgs(["--bogus"])).toThrow(
      "Unknown Indiana campaign finance flag: --bogus"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseIndianaCandidateFinanceSyncTriggerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });
});
