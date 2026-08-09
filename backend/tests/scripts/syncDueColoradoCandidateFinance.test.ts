import { describe, expect, it } from "vitest";

import {
  parseSyncDueColoradoCandidateFinanceScriptArgs,
  toSyncDueColoradoCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueColoradoCandidateFinance.js";

describe("syncDueColoradoCandidateFinance script", () => {
  it("parses due sync options", () => {
    expect(
      parseSyncDueColoradoCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/tracer",
        "--raw-zip=/tmp/tracer.zip",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawCacheDir: "/tmp/tracer",
      rawZipPath: "/tmp/tracer.zip",
    });
  });

  it("rejects unknown flags instead of silently ignoring typos", () => {
    expect(() => parseSyncDueColoradoCandidateFinanceScriptArgs(["--dryrun"])).toThrow(
      "Unknown Colorado candidate finance due sync flag: --dryrun"
    );
  });

  it("rejects boolean flags with explicit values", () => {
    expect(() => parseSyncDueColoradoCandidateFinanceScriptArgs(["--dry-run=true"])).toThrow(
      "Boolean flag does not accept a value: --dry-run"
    );
    expect(() => parseSyncDueColoradoCandidateFinanceScriptArgs(["--force=false"])).toThrow(
      "Boolean flag does not accept a value: --force"
    );
  });

  it("includes force in script output for audit logs", () => {
    const output = toSyncDueColoradoCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        dryRun: true,
        force: true,
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
        results: [],
      },
    });

    expect(output).toMatchObject({
      type: "colorado_candidate_finance_due_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      dry_run: true,
      force: true,
    });
    expect(typeof output.ts).toBe("string");
  });
});
