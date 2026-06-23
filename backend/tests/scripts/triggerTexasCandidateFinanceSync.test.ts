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
      rawDataZipPath: "/tmp/2026_ContributionData.csv.zip",
      rawDataCacheDir: "/tmp/texas-cache",
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
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

  it("can opt out of AI industry classification", () => {
    expect(parseTexasCandidateFinanceSyncTriggerArgs(["--no-ai-classify-industries"])).toMatchObject({
      aiClassifyIndustries: false,
    });
  });
});
