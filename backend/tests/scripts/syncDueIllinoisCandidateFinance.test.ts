import { describe, expect, it } from "vitest";

import {
  parseSyncDueIllinoisCandidateFinanceScriptArgs,
  toSyncDueIllinoisCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueIllinoisCandidateFinance.js";

describe("syncDueIllinoisCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueIllinoisCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--timeout-ms=45000",
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
      timeoutMs: 45000,
      aiClassifyIndustries: true,
      aiClassificationMinAmount: 25000,
    });
  });

  it("defaults to AI industry classification enabled", () => {
    expect(parseSyncDueIllinoisCandidateFinanceScriptArgs([])).toMatchObject({
      dryRun: false,
      force: false,
      aiClassifyIndustries: true,
    });
  });

  it("can opt out of AI industry classification", () => {
    expect(parseSyncDueIllinoisCandidateFinanceScriptArgs(["--no-ai-classify-industries"])).toMatchObject({
      aiClassifyIndustries: false,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueIllinoisCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueIllinoisCandidateFinanceScriptArgs(["--timeout-ms=0"])).toThrow(
      "Invalid --timeout-ms value"
    );
  });

  it("rejects missing and duplicate option values", () => {
    expect(() => parseSyncDueIllinoisCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() =>
      parseSyncDueIllinoisCandidateFinanceScriptArgs(["--max-candidates=10", "--max-candidates", "20"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("rejects unknown flags", () => {
    expect(() => parseSyncDueIllinoisCandidateFinanceScriptArgs(["--dryrun"])).toThrow("Unknown option: --dryrun");
  });

  it("formats script output", () => {
    const output = toSyncDueIllinoisCandidateFinanceScriptOutput({
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
            committeeKey: "FRIENDS OF JANE DOE",
            ok: true,
          },
        ],
      },
    });

    expect(output).toMatchObject({
      type: "illinois_candidate_finance_due_sync",
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
