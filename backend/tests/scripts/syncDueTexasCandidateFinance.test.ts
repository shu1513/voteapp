import { describe, expect, it } from "vitest";

import {
  parseSyncDueTexasCandidateFinanceScriptArgs,
  toSyncDueTexasCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueTexasCandidateFinance.js";

describe("syncDueTexasCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueTexasCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/texas-tec",
        "--raw-zip=/tmp/TEC_CF_CSV.zip",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
      rawCacheDir: "/tmp/texas-tec",
      rawZipPath: "/tmp/TEC_CF_CSV.zip",
    });
  });

  it("defaults to a disabled-by-flag safe option set", () => {
    expect(parseSyncDueTexasCandidateFinanceScriptArgs([])).toMatchObject({
      dryRun: false,
      force: false,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueTexasCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueTexasCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDueTexasCandidateFinanceScriptArgs(["--lookahead-days=5.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
  });

  it("rejects missing or blank option values", () => {
    expect(() => parseSyncDueTexasCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseSyncDueTexasCandidateFinanceScriptArgs(["--lookback-days", "--force"])).toThrow(
      "Missing --lookback-days value"
    );
    expect(() => parseSyncDueTexasCandidateFinanceScriptArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });

  it("rejects duplicate value flags", () => {
    expect(() =>
      parseSyncDueTexasCandidateFinanceScriptArgs(["--max-candidates=10", "--max-candidates", "20"])
    ).toThrow("Provide --max-candidates at most once");
    expect(() =>
      parseSyncDueTexasCandidateFinanceScriptArgs(["--raw-cache-dir=/tmp/a", "--raw-cache-dir=/tmp/b"])
    ).toThrow("Provide --raw-cache-dir at most once");
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseSyncDueTexasCandidateFinanceScriptArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });

  it("formats script output", () => {
    const output = toSyncDueTexasCandidateFinanceScriptOutput({
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
            committeeId: "00012345",
            ok: true,
            result: {
              candidateId: "candidate-1",
              electionId: "election-1",
              electionYear: 2026,
              dryRun: true,
              resolution: { status: "matched", committeeId: "00012345" },
              linkWritten: false,
              summaryWritten: false,
              directBreakdownsWritten: 0,
              outsideGroupsWritten: 0,
              outsideGroupBreakdownsWritten: 0,
              totalReceipts: 100,
              directContributionTotal: 90,
              outsideSupportTotal: null,
              outsideOpposeTotal: null,
              matchedContributionRowCount: 1,
              includedContributionRowCount: 1,
              skippedContributionRowCount: 0,
              matchedCandidateExpenditureRowCount: 0,
              includedCandidateExpenditureRowCount: 0,
              skippedCandidateExpenditureRowCount: 0,
              matchedOutsideContributionRowCount: 0,
              includedOutsideContributionRowCount: 0,
              skippedOutsideContributionRowCount: 0,
            },
          },
        ],
      },
    });

    expect(output).toMatchObject({
      type: "texas_candidate_finance_due_sync",
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
