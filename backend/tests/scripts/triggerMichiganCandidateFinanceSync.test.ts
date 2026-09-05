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

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseMichiganCandidateFinanceSyncTriggerArgs(["--max-candidates=10", "--max-candidates", "20"])).toThrow(
      "Provide --max-candidates at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseMichiganCandidateFinanceSyncTriggerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });

});
