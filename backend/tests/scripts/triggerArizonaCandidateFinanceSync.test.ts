import { describe, expect, it } from "vitest";

import { parseArizonaCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerArizonaCandidateFinanceSync.js";

describe("triggerArizonaCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseArizonaCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days=3",
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

  it("rejects malformed trigger options", () => {
    expect(() => parseArizonaCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseArizonaCandidateFinanceSyncTriggerArgs(["--outside-income-limit"])).toThrow(
      "Missing --outside-income-limit value"
    );
    expect(() => parseArizonaCandidateFinanceSyncTriggerArgs(["--min-industry-amount=-1"])).toThrow(
      "Invalid --min-industry-amount value"
    );
  });
});
