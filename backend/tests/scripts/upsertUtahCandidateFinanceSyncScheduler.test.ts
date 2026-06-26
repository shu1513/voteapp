import { describe, expect, it } from "vitest";

import { parseUpsertUtahCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertUtahCandidateFinanceSyncScheduler.js";

describe("upsertUtahCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertUtahCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/utah-cache",
        "--refresh-cache",
        "--ai-classify-industries",
        "--ai-min-amount=5000",
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
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 5000,
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseUpsertUtahCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertUtahCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

  it("rejects duplicate flag values", () => {
    expect(() =>
      parseUpsertUtahCandidateFinanceSyncSchedulerArgs(["--max-candidates=5", "--max-candidates", "10"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("does not enable AI industry classification by default", () => {
    expect(parseUpsertUtahCandidateFinanceSyncSchedulerArgs([])).toMatchObject({
      aiClassifyIndustries: false,
    });
  });
});
