import { describe, expect, it } from "vitest";

import { parseUpsertWisconsinCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertWisconsinCandidateFinanceSyncScheduler.js";

describe("upsertWisconsinCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertWisconsinCandidateFinanceSyncSchedulerArgs([
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

  it("parses AI classification opt-out", () => {
    expect(parseUpsertWisconsinCandidateFinanceSyncSchedulerArgs(["--no-ai-classify-industries"])).toEqual({
      dryRun: false,
      force: false,
      maxCandidates: undefined,
      staleAfterDays: undefined,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
      aiClassifyIndustries: false,
      aiClassificationMinAmount: undefined,
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseUpsertWisconsinCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertWisconsinCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

  it("rejects duplicate flag values", () => {
    expect(() =>
      parseUpsertWisconsinCandidateFinanceSyncSchedulerArgs(["--max-candidates=5", "--max-candidates", "10"])
    ).toThrow("Provide --max-candidates at most once");
  });
});
