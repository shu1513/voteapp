import { describe, expect, it } from "vitest";

import { parseUpsertOregonCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertOregonCandidateFinanceSyncScheduler.js";

describe("upsertOregonCandidateFinanceSyncScheduler script", () => {
  it("parses Oregon finance scheduler flags", () => {
    expect(
      parseUpsertOregonCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days",
        "90",
        "--direct-max-breakdowns=10",
        "--outside-max-groups=8",
        "--outside-max-breakdowns=12",
        "--min-industry-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 90,
      directMaxBreakdownsPerCategory: 10,
      outsideMaxGroups: 8,
      outsideMaxBreakdownsPerCategory: 12,
      minIndustryAmount: 25000,
    });
  });

  it("rejects invalid or duplicate numeric flags", () => {
    expect(() => parseUpsertOregonCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value: 5x"
    );
    expect(() => parseUpsertOregonCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() =>
      parseUpsertOregonCandidateFinanceSyncSchedulerArgs(["--outside-max-groups=5", "--outside-max-groups", "10"])
    ).toThrow("Provide --outside-max-groups at most once");
  });
});
