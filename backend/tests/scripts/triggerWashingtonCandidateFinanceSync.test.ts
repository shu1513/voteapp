import { describe, expect, it } from "vitest";

import { parseWashingtonCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerWashingtonCandidateFinanceSync.js";

describe("triggerWashingtonCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseWashingtonCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
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
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseWashingtonCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseWashingtonCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

  it("rejects duplicate flag values", () => {
    expect(() =>
      parseWashingtonCandidateFinanceSyncTriggerArgs(["--max-candidates=5", "--max-candidates", "10"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("can opt out of AI industry classification", () => {
    expect(parseWashingtonCandidateFinanceSyncTriggerArgs(["--no-ai-classify-industries"])).toMatchObject({
      aiClassifyIndustries: false,
    });
  });
});
