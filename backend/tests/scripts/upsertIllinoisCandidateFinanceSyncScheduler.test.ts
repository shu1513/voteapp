import { describe, expect, it } from "vitest";

import { parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertIllinoisCandidateFinanceSyncScheduler.js";

describe("upsertIllinoisCandidateFinanceSyncScheduler script", () => {
  it("keeps recurring AI industry classification opt-in", () => {
    expect(parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs([])).toEqual({
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

  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs([
        "--max-candidates",
        "10",
        "--stale-after-days=3",
        "--lookback-days=7",
        "--lookahead-days=180",
        "--ai-classify-industries",
        "--ai-min-amount=25000",
      ])
    ).toEqual({
      dryRun: false,
      force: false,
      maxCandidates: 10,
      staleAfterDays: 3,
      electionLookbackDays: 7,
      electionLookaheadDays: 180,
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("rejects malformed numeric flags", () => {
    expect(() => parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs(["--lookahead-days=1.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
  });
});
