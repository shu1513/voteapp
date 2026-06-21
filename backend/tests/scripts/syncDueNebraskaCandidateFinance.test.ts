import { describe, expect, it } from "vitest";

import {
  parseSyncDueNebraskaCandidateFinanceScriptArgs,
  toSyncDueNebraskaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueNebraskaCandidateFinance.js";

describe("syncDueNebraskaCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueNebraskaCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/nadc",
        "--raw-zip=/tmp/2026.zip",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
      rawCacheDir: "/tmp/nadc",
      rawZipPath: "/tmp/2026.zip",
    });
  });

  it("defaults to a disabled-by-flag safe option set", () => {
    expect(parseSyncDueNebraskaCandidateFinanceScriptArgs([])).toMatchObject({
      dryRun: false,
      force: false,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueNebraskaCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueNebraskaCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDueNebraskaCandidateFinanceScriptArgs(["--lookahead-days=5.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
  });

  it("rejects missing or blank option values", () => {
    expect(() => parseSyncDueNebraskaCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseSyncDueNebraskaCandidateFinanceScriptArgs(["--lookback-days", "--force"])).toThrow(
      "Missing --lookback-days value"
    );
    expect(() => parseSyncDueNebraskaCandidateFinanceScriptArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });

  it("formats script output", () => {
    const output = toSyncDueNebraskaCandidateFinanceScriptOutput({
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
        results: [
          {
            candidateId: "candidate-1",
            electionId: "election-1",
            electionYear: 2026,
            committeeId: "7569",
            ok: true,
            result: {
              candidateId: "candidate-1",
              electionId: "election-1",
              electionYear: 2026,
              dryRun: true,
              resolution: { status: "matched", committeeId: "7569" },
              linkWritten: false,
              summaryWritten: false,
              directBreakdownsWritten: 0,
              totalReceipts: 100,
              directContributionTotal: 90,
              matchedContributionRowCount: 1,
              includedContributionRowCount: 1,
              skippedContributionRowCount: 0,
            },
          },
        ],
      },
    });

    expect(output).toMatchObject({
      type: "nebraska_candidate_finance_due_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      dry_run: true,
      result: {
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
        results: [
          {
            result: {
              totalReceipts: 100,
              directContributionTotal: 90,
            },
          },
        ],
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
