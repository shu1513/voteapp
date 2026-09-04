import { describe, expect, it } from "vitest";

import { parseSyncDueIdahoCandidateFinanceScriptArgs } from "../../src/scripts/syncDueIdahoCandidateFinance.js";

describe("parseSyncDueIdahoCandidateFinanceScriptArgs", () => {
  it("parses defaults and explicit flags", () => {
    expect(parseSyncDueIdahoCandidateFinanceScriptArgs([])).toEqual({
      dryRun: false,
      force: false,
      autoLinkMissingLinks: true,
      maxCandidates: undefined,
      staleAfterDays: undefined,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
    });
    expect(
      parseSyncDueIdahoCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--no-auto-link",
        "--max-candidates=200",
        "--stale-after-days",
        "1",
        "--lookback-days=10",
        "--lookahead-days=800",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      autoLinkMissingLinks: false,
      maxCandidates: 200,
      staleAfterDays: 1,
      electionLookbackDays: 10,
      electionLookaheadDays: 800,
    });
  });

  it("rejects unknown flags, positionals, and bad values", () => {
    expect(() => parseSyncDueIdahoCandidateFinanceScriptArgs(["--dryrun"])).toThrow(
      "Unknown Idaho candidate finance due sync flag: --dryrun"
    );
    expect(() => parseSyncDueIdahoCandidateFinanceScriptArgs(["dry-run"])).toThrow("Unexpected positional argument");
    expect(() => parseSyncDueIdahoCandidateFinanceScriptArgs(["--dry-run=true"])).toThrow("does not accept a value");
    expect(() => parseSyncDueIdahoCandidateFinanceScriptArgs(["--max-candidates"])).toThrow("Missing --max-candidates value");
    expect(() => parseSyncDueIdahoCandidateFinanceScriptArgs(["--max-candidates=0"])).toThrow("Invalid --max-candidates value: 0");
    expect(() => parseSyncDueIdahoCandidateFinanceScriptArgs(["--stale-after-days=1", "--stale-after-days=2"])).toThrow(
      "at most once"
    );
  });
});
