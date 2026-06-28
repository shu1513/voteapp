import { describe, expect, it } from "vitest";

import {
  parseSyncDueKentuckyCandidateFinanceScriptArgs,
  toSyncDueKentuckyCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueKentuckyCandidateFinance.js";

describe("syncDueKentuckyCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueKentuckyCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
    });
  });

  it("defaults to a disabled-by-flag safe option set", () => {
    expect(parseSyncDueKentuckyCandidateFinanceScriptArgs([])).toEqual({
      dryRun: false,
      force: false,
      maxCandidates: undefined,
      staleAfterDays: undefined,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueKentuckyCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueKentuckyCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDueKentuckyCandidateFinanceScriptArgs(["--lookahead-days=5.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
  });

  it("rejects missing and duplicate option values", () => {
    expect(() => parseSyncDueKentuckyCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseSyncDueKentuckyCandidateFinanceScriptArgs(["--lookback-days", "--force"])).toThrow(
      "Missing --lookback-days value"
    );
    expect(() =>
      parseSyncDueKentuckyCandidateFinanceScriptArgs(["--max-candidates=10", "--max-candidates", "20"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("rejects unknown flags instead of silently ignoring typos", () => {
    expect(() => parseSyncDueKentuckyCandidateFinanceScriptArgs(["--dryrun"])).toThrow(
      "Unknown Kentucky candidate finance due sync flag: --dryrun"
    );
  });

  it("formats script output", () => {
    const output = toSyncDueKentuckyCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        dryRun: true,
        force: false,
        maxCandidates: 2,
      },
      result: {
        dryRun: true,
        now: "2026-01-02T03:04:05.000Z",
        staleAfterDays: 7,
        maxCandidates: 2,
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
        syncedCandidateCount: 1,
        failedCandidateCount: 1,
        autoLinkAttemptedCount: 0,
        autoLinkLinkedCount: 0,
        results: [
          {
            candidateId: "candidate-1",
            electionId: "election-1",
            electionYear: 2023,
            candidateKey: "ANDY BESHEAR|GOVERNOR|STATEWIDE|2023-11-07",
            committeeKey: "BESHEAR CAMPAIGN COMMITTEE",
            ok: true,
          },
        ],
      },
    });

    expect(output).toMatchObject({
      type: "kentucky_candidate_finance_due_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      dry_run: true,
      result: {
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
        autoLinkAttemptedCount: 0,
        autoLinkLinkedCount: 0,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
