import { describe, expect, it } from "vitest";

import {
  parseSyncDueNewHampshireCandidateFinanceScriptArgs,
  toSyncDueNewHampshireCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueNewHampshireCandidateFinance.js";

describe("parseSyncDueNewHampshireCandidateFinanceScriptArgs", () => {
  it("parses defaults and explicit flags", () => {
    expect(parseSyncDueNewHampshireCandidateFinanceScriptArgs([])).toEqual({
      dryRun: false,
      force: false,
      autoLinkMissingLinks: true,
      maxCandidates: undefined,
      staleAfterDays: undefined,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
    });
    expect(
      parseSyncDueNewHampshireCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--no-auto-link",
        "--max-candidates=200",
        "--stale-after-days",
        "1",
        "--lookback-days=10",
        "--lookahead-days=800",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      autoLinkMissingLinks: false,
      maxCandidates: 200,
      staleAfterDays: 1,
      electionLookbackDays: 10,
      electionLookaheadDays: 800,
    });
  });

  it("rejects unknown flags, positionals, and bad values", () => {
    expect(() => parseSyncDueNewHampshireCandidateFinanceScriptArgs(["--dryrun"])).toThrow(
      "Unknown New Hampshire candidate finance due sync flag: --dryrun"
    );
    expect(() => parseSyncDueNewHampshireCandidateFinanceScriptArgs(["dry-run"])).toThrow("Unexpected positional argument");
    expect(() => parseSyncDueNewHampshireCandidateFinanceScriptArgs(["--dry-run=true"])).toThrow("does not accept a value");
    expect(() => parseSyncDueNewHampshireCandidateFinanceScriptArgs(["--max-candidates"])).toThrow(
      "Missing --max-candidates value"
    );
    expect(() => parseSyncDueNewHampshireCandidateFinanceScriptArgs(["--max-candidates=0"])).toThrow(
      "Invalid --max-candidates value: 0"
    );
    expect(() =>
      parseSyncDueNewHampshireCandidateFinanceScriptArgs(["--stale-after-days=1", "--stale-after-days=2"])
    ).toThrow("at most once");
  });
});

describe("toSyncDueNewHampshireCandidateFinanceScriptOutput", () => {
  it("wraps the batch result with the run envelope", () => {
    const startedAt = new Date("2026-09-03T00:00:00.000Z");
    const result = {
      dryRun: true,
      autoLinkResults: [],
      totalDueRows: 0,
      attempted: 0,
      succeeded: 0,
      failed: 0,
      electionCycleIds: {},
      candidates: [],
    };
    expect(
      toSyncDueNewHampshireCandidateFinanceScriptOutput({
        startedAt,
        options: parseSyncDueNewHampshireCandidateFinanceScriptArgs(["--dry-run"]),
        result,
      })
    ).toMatchObject({
      type: "new_hampshire_candidate_finance_due_sync",
      started_at: "2026-09-03T00:00:00.000Z",
      dry_run: true,
      result,
    });
  });
});
