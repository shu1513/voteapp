import { describe, expect, it } from "vitest";

import { parseNewMexicoCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerNewMexicoCandidateFinanceSync.js";

describe("triggerNewMexicoCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseNewMexicoCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/cfis",
        "--ai-classify-industries",
        "--ai-min-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/cfis",
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseNewMexicoCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseNewMexicoCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });
});
