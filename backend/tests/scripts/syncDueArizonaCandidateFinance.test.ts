import { describe, expect, it } from "vitest";

import {
  parseSyncDueArizonaCandidateFinanceScriptArgs,
  toSyncDueArizonaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueArizonaCandidateFinance.js";

describe("syncDueArizonaCandidateFinance script", () => {
  it("parses due sync options", () => {
    expect(
      parseSyncDueArizonaCandidateFinanceScriptArgs([
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

  it("rejects malformed options", () => {
    expect(() => parseSyncDueArizonaCandidateFinanceScriptArgs(["--max-candidates=0"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueArizonaCandidateFinanceScriptArgs(["--min-industry-amount=-1"])).toThrow(
      "Invalid --min-industry-amount value"
    );
    expect(() => parseSyncDueArizonaCandidateFinanceScriptArgs(["--timeout-ms"])).toThrow("Missing --timeout-ms value");
  });

  it("formats output", () => {
    const output = toSyncDueArizonaCandidateFinanceScriptOutput({
      startedAt: new Date("2026-06-25T12:00:00.000Z"),
      options: {
        dryRun: true,
        force: false,
      },
      result: {
        dryRun: true,
        now: "2026-06-25T12:00:00.000Z",
        staleAfterDays: 7,
        maxCandidates: 25,
        dueCandidateCount: 0,
        selectedCandidateCount: 0,
        syncedCandidateCount: 0,
        failedCandidateCount: 0,
        autoLinkAttemptedCount: 0,
        autoLinkLinkedCount: 0,
        results: [],
      },
    });
    expect(output).toMatchObject({
      type: "arizona_candidate_finance_due_sync",
      started_at: "2026-06-25T12:00:00.000Z",
      dry_run: true,
      result: {
        dueCandidateCount: 0,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
