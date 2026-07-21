import { describe, expect, it } from "vitest";

import { parseMichiganCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerMichiganCandidateFinanceSync.js";

describe("triggerMichiganCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseMichiganCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-extracted-dir=/tmp/2022_mi_cfr",
        "--raw-cache-dir=/tmp/michigan-cache",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataExtractedDir: "/tmp/2022_mi_cfr",
      rawDataCacheDir: "/tmp/michigan-cache",
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseMichiganCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseMichiganCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseMichiganCandidateFinanceSyncTriggerArgs(["--raw-extracted-dir="])).toThrow(
      "Missing --raw-extracted-dir value"
    );
    expect(() => parseMichiganCandidateFinanceSyncTriggerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });

});
