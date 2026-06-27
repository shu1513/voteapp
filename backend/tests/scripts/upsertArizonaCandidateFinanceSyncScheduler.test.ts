import { describe, expect, it } from "vitest";

import { parseUpsertArizonaCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertArizonaCandidateFinanceSyncScheduler.js";

describe("upsertArizonaCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertArizonaCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--timeout-ms=5000",
        "--income-limit=100",
        "--ie-limit=50",
        "--outside-income-limit=75",
        "--outside-max-groups=4",
        "--direct-max-breakdowns=10",
        "--outside-max-breakdowns=8",
        "--min-industry-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      timeoutMs: 5000,
      directIncomeLimit: 100,
      independentExpenditureLimitPerPosition: 50,
      outsideGroupIncomeLimitPerGroup: 75,
      outsideMaxGroups: 4,
      directMaxBreakdownsPerCategory: 10,
      outsideMaxBreakdownsPerCategory: 8,
      minIndustryAmount: 25000,
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseUpsertArizonaCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertArizonaCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseUpsertArizonaCandidateFinanceSyncSchedulerArgs(["--min-industry-amount=-1"])).toThrow(
      "Invalid --min-industry-amount value"
    );
  });

  it("rejects duplicate flag values", () => {
    expect(() =>
      parseUpsertArizonaCandidateFinanceSyncSchedulerArgs(["--max-candidates=5", "--max-candidates", "10"])
    ).toThrow("Provide --max-candidates at most once");
  });
});
