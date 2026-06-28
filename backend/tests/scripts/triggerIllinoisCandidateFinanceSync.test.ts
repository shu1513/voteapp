import { describe, expect, it } from "vitest";

import { parseIllinoisCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerIllinoisCandidateFinanceSync.js";

describe("triggerIllinoisCandidateFinanceSync script", () => {
  it("parses manual scheduler trigger flags", () => {
    expect(
      parseIllinoisCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days=2",
        "--lookback-days=14",
        "--lookahead-days=365",
        "--no-ai-classify-industries",
        "--ai-min-amount=50000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 2,
      electionLookbackDays: 14,
      electionLookaheadDays: 365,
      aiClassifyIndustries: false,
      aiClassificationMinAmount: 50000,
    });
  });

  it("rejects unknown flags", () => {
    expect(() => parseIllinoisCandidateFinanceSyncTriggerArgs(["--unknown"])).toThrow("Unknown option: --unknown");
  });

  it("rejects valued booleans and conflicting AI flags", () => {
    expect(() => parseIllinoisCandidateFinanceSyncTriggerArgs(["--dry-run=true"])).toThrow(
      "Boolean flag --dry-run does not take a value"
    );
    expect(() => parseIllinoisCandidateFinanceSyncTriggerArgs(["--dry-run", "false"])).toThrow(
      "Boolean flag --dry-run does not take a value"
    );
    expect(() =>
      parseIllinoisCandidateFinanceSyncTriggerArgs(["--ai-classify-industries", "--no-ai-classify-industries"])
    ).toThrow("Provide --ai-classify-industries or --no-ai-classify-industries, not both");
  });
});
