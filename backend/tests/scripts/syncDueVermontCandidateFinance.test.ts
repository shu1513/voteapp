import { describe, expect, it } from "vitest";

import {
  parseSyncDueVermontCandidateFinanceScriptArgs,
  toSyncDueVermontCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueVermontCandidateFinance.js";

describe("syncDueVermontCandidateFinance script", () => {
  it("parses due-sync flags", () => {
    expect(
      parseSyncDueVermontCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=10",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=400",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 10,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 400,
    });
  });

  it("rejects invalid or repeated positive integer flags", () => {
    expect(() => parseSyncDueVermontCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueVermontCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() =>
      parseSyncDueVermontCandidateFinanceScriptArgs(["--max-candidates=10", "--max-candidates", "20"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("formats script output", () => {
    const output = toSyncDueVermontCandidateFinanceScriptOutput({
      startedAt: new Date("2026-06-01T00:00:00.000Z"),
      options: { dryRun: true, force: false },
      result: {
        dryRun: true,
        now: "2026-06-01T00:00:00.000Z",
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
      type: "vermont_candidate_finance_due_sync",
      started_at: "2026-06-01T00:00:00.000Z",
      dry_run: true,
      result: { selectedCandidateCount: 0 },
    });
  });
});
