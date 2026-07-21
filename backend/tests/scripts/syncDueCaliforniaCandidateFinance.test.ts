import { describe, expect, it } from "vitest";

import {
  parseSyncDueCaliforniaCandidateFinanceScriptArgs,
  toSyncDueCaliforniaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueCaliforniaCandidateFinance.js";

describe("syncDueCaliforniaCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueCaliforniaCandidateFinanceScriptArgs([
        "--dry-run",
        "--skip-outside",
        "--force",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--timeout-ms=5000",
        "--raw-cache-dir=/tmp/calaccess",
      ])
    ).toEqual({
      dryRun: true,
      includeOutside: false,
      force: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
      timeoutMs: 5000,
      rawZipPath: undefined,
      rawCacheDir: "/tmp/calaccess",
    });
  });

  it("defaults to outside-spending sync enabled", () => {
    expect(parseSyncDueCaliforniaCandidateFinanceScriptArgs([])).toMatchObject({
      dryRun: false,
      includeOutside: true,
      force: false,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueCaliforniaCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueCaliforniaCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDueCaliforniaCandidateFinanceScriptArgs(["--timeout-ms=5.5"])).toThrow(
      "Invalid --timeout-ms value"
    );
  });

  it("formats script output", () => {
    const output = toSyncDueCaliforniaCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        dryRun: true,
        includeOutside: true,
        force: false,
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
      type: "california_candidate_finance_due_sync",
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
