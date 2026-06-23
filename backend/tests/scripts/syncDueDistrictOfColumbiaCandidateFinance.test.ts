import { describe, expect, it } from "vitest";

import {
  parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs,
  toSyncDueDistrictOfColumbiaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueDistrictOfColumbiaCandidateFinance.js";

describe("syncDueDistrictOfColumbiaCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--ai-classify-industries",
        "--ai-min-amount=25000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("defaults to AI industry classification enabled", () => {
    expect(parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs([])).toMatchObject({
      dryRun: false,
      force: false,
      aiClassifyIndustries: true,
    });
  });

  it("can opt out of AI industry classification", () => {
    expect(parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs(["--no-ai-classify-industries"])).toMatchObject({
      aiClassifyIndustries: false,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs(["--lookahead-days=5.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
  });

  it("rejects missing and duplicate option values", () => {
    expect(() => parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs(["--lookback-days", "--force"])).toThrow(
      "Missing --lookback-days value"
    );
    expect(() =>
      parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs(["--max-candidates=10", "--max-candidates", "20"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("rejects unknown flags", () => {
    expect(() => parseSyncDueDistrictOfColumbiaCandidateFinanceScriptArgs(["--dryrun"])).toThrow(
      "Unknown option: --dryrun"
    );
  });

  it("formats script output", () => {
    const output = toSyncDueDistrictOfColumbiaCandidateFinanceScriptOutput({
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
        autoLinkAttemptedCount: 2,
        autoLinkLinkedCount: 1,
        results: [
          {
            candidateId: "candidate-1",
            electionId: "election-1",
            electionYear: 2026,
            committeeKey: "COMMITTEE TO ELECT JANE DOE",
            ok: true,
          },
        ],
      },
    });

    expect(output).toMatchObject({
      type: "district_of_columbia_candidate_finance_due_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      dry_run: true,
      ai_classify_industries: false,
      result: {
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
        autoLinkAttemptedCount: 2,
        autoLinkLinkedCount: 1,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
