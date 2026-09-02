import { describe, expect, it } from "vitest";

import { parseSyncDueArkansasCandidateFinanceScriptArgs } from "../../../src/scripts/syncDueArkansasCandidateFinance.js";

describe("parseSyncDueArkansasCandidateFinanceScriptArgs", () => {
  it("parses the supported flags with unset numbers left to the batch defaults", () => {
    expect(parseSyncDueArkansasCandidateFinanceScriptArgs([])).toEqual({
      dryRun: false,
      force: false,
      dnsFallback: false,
      autoLinkMissingLinks: true,
      maxCandidates: undefined,
      staleAfterDays: undefined,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
    });
    expect(
      parseSyncDueArkansasCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--dns-fallback",
        "--no-auto-link",
        "--max-candidates=3",
        "--stale-after-days",
        "1",
        "--lookback-days=40",
        "--lookahead-days=800",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      dnsFallback: true,
      autoLinkMissingLinks: false,
      maxCandidates: 3,
      staleAfterDays: 1,
      electionLookbackDays: 40,
      electionLookaheadDays: 800,
    });
  });

  it("rejects unknown flags and bad numbers", () => {
    expect(() => parseSyncDueArkansasCandidateFinanceScriptArgs(["--cache-dir=x"])).toThrow(/cache-dir/);
    expect(() => parseSyncDueArkansasCandidateFinanceScriptArgs(["--max-candidates=0"])).toThrow(/--max-candidates/);
  });
});
