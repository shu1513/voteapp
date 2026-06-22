import { describe, expect, it } from "vitest";

import {
  parseSyncDueOklahomaCandidateFinanceScriptArgs,
  toSyncDueOklahomaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueOklahomaCandidateFinance.js";

describe("syncDueOklahomaCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueOklahomaCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/guardian",
        "--raw-zip=/tmp/2026.zip",
        "--skip-outside",
        "--ai-min-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
      rawCacheDir: "/tmp/guardian",
      rawZipPath: "/tmp/2026.zip",
      includeOutside: false,
      aiClassifyIndustries: false,
      aiClassificationMinAmount: 25000,
    });
  });

  it("defaults to a disabled-by-flag safe option set", () => {
    expect(parseSyncDueOklahomaCandidateFinanceScriptArgs([])).toMatchObject({
      dryRun: false,
      force: false,
      includeOutside: true,
      aiClassifyIndustries: false,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueOklahomaCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueOklahomaCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDueOklahomaCandidateFinanceScriptArgs(["--lookahead-days=5.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
  });

  it("rejects missing or blank option values", () => {
    expect(() => parseSyncDueOklahomaCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseSyncDueOklahomaCandidateFinanceScriptArgs(["--lookback-days", "--force"])).toThrow(
      "Missing --lookback-days value"
    );
    expect(() => parseSyncDueOklahomaCandidateFinanceScriptArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
  });

  it("rejects duplicate value flags", () => {
    expect(() =>
      parseSyncDueOklahomaCandidateFinanceScriptArgs(["--max-candidates=10", "--max-candidates", "20"])
    ).toThrow("Provide --max-candidates at most once");
    expect(() =>
      parseSyncDueOklahomaCandidateFinanceScriptArgs(["--raw-cache-dir=/tmp/a", "--raw-cache-dir=/tmp/b"])
    ).toThrow("Provide --raw-cache-dir at most once");
  });

  it("formats script output", () => {
    const output = toSyncDueOklahomaCandidateFinanceScriptOutput({
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
            committeeId: "11954",
            ok: true,
            result: {
              candidateId: "candidate-1",
              electionId: "election-1",
              electionYear: 2026,
              dryRun: true,
              resolution: { status: "matched", committeeId: "11954" },
              linkWritten: false,
              summaryWritten: false,
              directBreakdownsWritten: 0,
              outsideIncluded: false,
              outsideGroupsWritten: 0,
              totalReceipts: 100,
              directContributionTotal: 90,
              outsideSupportTotal: null,
              outsideOpposeTotal: null,
              outsideReportsExamined: 0,
              outsideUsableReports: 0,
              outsideSkippedReports: 0,
              matchedContributionRowCount: 1,
              includedContributionRowCount: 1,
              skippedContributionRowCount: 0,
            },
          },
        ],
      },
    });

    expect(output).toMatchObject({
      type: "oklahoma_candidate_finance_due_sync",
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
