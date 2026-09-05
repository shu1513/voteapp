import { describe, expect, it } from "vitest";

import { parseOklahomaCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerOklahomaCandidateFinanceSync.js";

describe("triggerOklahomaCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseOklahomaCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/guardian",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/guardian",
    });
  });

  it("rejects malformed, missing, and duplicate value flags", () => {
    expect(() => parseOklahomaCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseOklahomaCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() =>
      parseOklahomaCandidateFinanceSyncTriggerArgs(["--max-candidates=5", "--max-candidates", "6"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseOklahomaCandidateFinanceSyncTriggerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });
});
