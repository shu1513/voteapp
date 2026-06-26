import { describe, expect, it } from "vitest";

import { parseUpsertMarylandCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertMarylandCandidateFinanceSyncScheduler.js";

describe("upsertMarylandCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler payload options", () => {
    expect(
      parseUpsertMarylandCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=20",
        "--stale-after-days=4",
        "--lookback-days",
        "30",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/maryland-cfs",
        "--ai-min-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 20,
      staleAfterDays: 4,
      electionLookbackDays: 30,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/maryland-cfs",
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("can opt out of AI industry classification", () => {
    expect(parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--no-ai-classify-industries"])).toMatchObject({
      aiClassifyIndustries: false,
    });
  });

  it("rejects malformed scheduler payload options", () => {
    expect(() => parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseUpsertMarylandCandidateFinanceSyncSchedulerArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });
});
