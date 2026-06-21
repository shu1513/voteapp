import { describe, expect, it } from "vitest";

import { parseConnecticutCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerConnecticutCandidateFinanceSync.js";

describe("triggerConnecticutCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseConnecticutCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/ecris",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/ecris",
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseConnecticutCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseConnecticutCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });
});
