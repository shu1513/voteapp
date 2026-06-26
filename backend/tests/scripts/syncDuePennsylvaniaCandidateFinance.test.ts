import { describe, expect, it } from "vitest";

import {
  parseSyncDuePennsylvaniaCandidateFinanceScriptArgs,
  toSyncDuePennsylvaniaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDuePennsylvaniaCandidateFinance.js";

describe("syncDuePennsylvaniaCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDuePennsylvaniaCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/pa-cf",
        "--raw-extracted-dir=/tmp/pa-cf/2022",
        "--ai-min-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
      rawCacheDir: "/tmp/pa-cf",
      rawExtractedDir: "/tmp/pa-cf/2022",
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("defaults to AI industry classification enabled", () => {
    expect(parseSyncDuePennsylvaniaCandidateFinanceScriptArgs([])).toMatchObject({
      dryRun: false,
      force: false,
      aiClassifyIndustries: true,
    });
  });

  it("can opt out of AI industry classification", () => {
    expect(parseSyncDuePennsylvaniaCandidateFinanceScriptArgs(["--no-ai-classify-industries"])).toMatchObject({
      aiClassifyIndustries: false,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDuePennsylvaniaCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDuePennsylvaniaCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDuePennsylvaniaCandidateFinanceScriptArgs(["--lookahead-days=5.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
  });

  it("rejects missing or duplicate option values", () => {
    expect(() => parseSyncDuePennsylvaniaCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseSyncDuePennsylvaniaCandidateFinanceScriptArgs(["--lookback-days", "--force"])).toThrow(
      "Missing --lookback-days value"
    );
    expect(() => parseSyncDuePennsylvaniaCandidateFinanceScriptArgs(["--raw-cache-dir", "   "])).toThrow(
      "Missing --raw-cache-dir value"
    );
    expect(() =>
      parseSyncDuePennsylvaniaCandidateFinanceScriptArgs([
        "--raw-extracted-dir=/tmp/a",
        "--raw-extracted-dir=/tmp/b",
      ])
    ).toThrow("Provide --raw-extracted-dir at most once");
  });

  it("formats script output", () => {
    const output = toSyncDuePennsylvaniaCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        dryRun: true,
        force: false,
        maxCandidates: 2,
        aiClassifyIndustries: false,
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
      type: "pennsylvania_candidate_finance_due_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      dry_run: true,
      ai_classify_industries: false,
      result: {
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
