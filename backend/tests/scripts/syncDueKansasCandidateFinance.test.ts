import { describe, expect, it } from "vitest";

import { parseSyncDueKansasCandidateFinanceScriptArgs } from "../../src/scripts/syncDueKansasCandidateFinance.js";

describe("parseSyncDueKansasCandidateFinanceScriptArgs", () => {
  it("parses the supported flags with unset numbers left to the batch defaults", () => {
    expect(parseSyncDueKansasCandidateFinanceScriptArgs([])).toEqual({
      dryRun: false,
      force: false,
      maxCandidates: undefined,
      staleAfterDays: undefined,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
    });
    expect(
      parseSyncDueKansasCandidateFinanceScriptArgs(["--dry-run", "--force", "--max-candidates=3", "--stale-after-days", "1", "--lookback-days=40", "--lookahead-days=800"])
    ).toEqual({ dryRun: true, force: true, maxCandidates: 3, staleAfterDays: 1, electionLookbackDays: 40, electionLookaheadDays: 800 });
  });

  it("rejects unknown flags, positionals, and bad numbers", () => {
    expect(() => parseSyncDueKansasCandidateFinanceScriptArgs(["--aggregate"])).toThrow("Unknown Kansas candidate finance due sync flag");
    expect(() => parseSyncDueKansasCandidateFinanceScriptArgs(["dry-run"])).toThrow("Unexpected positional argument");
    expect(() => parseSyncDueKansasCandidateFinanceScriptArgs(["--max-candidates=0"])).toThrow("Invalid --max-candidates value");
    expect(() => parseSyncDueKansasCandidateFinanceScriptArgs(["--max-candidates=1", "--max-candidates=2"])).toThrow("at most once");
  });
});
