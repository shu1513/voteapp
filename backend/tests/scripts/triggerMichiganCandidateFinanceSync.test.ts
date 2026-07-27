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
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseMichiganCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseMichiganCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

});
