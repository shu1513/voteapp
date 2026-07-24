import { describe, expect, it } from "vitest";

import {
  parseSyncDueCandidateFinanceScriptArgs,
  toSyncDueCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueCandidateFinance.js";

describe("syncDueCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueCandidateFinanceScriptArgs([
        "--dry-run",
        "--include-outside",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--per-page=25",
        "--top-groups",
        "8",
        "--timeout-ms=5000",
        "--request-interval-ms=3700",
      ])
    ).toEqual({
      dryRun: true,
      includeOutside: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
      perPage: 25,
      outsideGroupLimit: 8,
      timeoutMs: 5000,
      requestIntervalMs: 3700,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDueCandidateFinanceScriptArgs(["--request-interval-ms=-1"])).toThrow(
      "Invalid --request-interval-ms value"
    );
    expect(() => parseSyncDueCandidateFinanceScriptArgs(["--request-interval-ms=1.5"])).toThrow(
      "Invalid --request-interval-ms value"
    );
  });

  it("allows explicitly disabling request pacing", () => {
    expect(parseSyncDueCandidateFinanceScriptArgs(["--request-interval-ms=0"])).toMatchObject({
      requestIntervalMs: 0,
    });
  });

  it("formats script output", () => {
    const output = toSyncDueCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        dryRun: true,
        includeOutside: true,
        maxCandidates: 2,
      },
      result: {
        dryRun: true,
        includeOutside: true,
        now: "2026-01-02T03:04:05.000Z",
        staleAfterDays: 7,
        maxCandidates: 2,
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
        syncedCandidateCount: 1,
        failedCandidateCount: 1,
        results: [],
      },
    });

    expect(output).toMatchObject({
      type: "candidate_finance_due_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      dry_run: true,
      include_outside: true,
      result: {
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
