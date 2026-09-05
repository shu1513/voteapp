import { describe, expect, it } from "vitest";

import { parseAutoLinkNewHampshireCandidateFinanceScriptArgs } from "../../src/scripts/autoLinkNewHampshireCandidateFinance.js";

describe("parseAutoLinkNewHampshireCandidateFinanceScriptArgs", () => {
  it("parses defaults and explicit flags", () => {
    expect(parseAutoLinkNewHampshireCandidateFinanceScriptArgs([])).toEqual({
      force: false,
      dryRun: false,
      maxCandidates: null,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });
    expect(
      parseAutoLinkNewHampshireCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=200",
        "--lookback-days",
        "10",
      ])
    ).toEqual({ force: true, dryRun: true, maxCandidates: 200, electionLookbackDays: 10, electionLookaheadDays: 730 });
  });

  it("rejects unknown flags, positionals, and bad values", () => {
    expect(() => parseAutoLinkNewHampshireCandidateFinanceScriptArgs(["--page-size=5"])).toThrow(
      "Unknown New Hampshire candidate finance auto-link flag"
    );
    expect(() => parseAutoLinkNewHampshireCandidateFinanceScriptArgs(["dry-run"])).toThrow("Unexpected positional argument");
    expect(() => parseAutoLinkNewHampshireCandidateFinanceScriptArgs(["--dry-run=true"])).toThrow("does not accept a value");
    expect(() => parseAutoLinkNewHampshireCandidateFinanceScriptArgs(["--max-candidates=0"])).toThrow("Invalid --max-candidates");
    expect(() => parseAutoLinkNewHampshireCandidateFinanceScriptArgs(["--max-candidates=1", "--max-candidates=2"])).toThrow(
      "at most once"
    );
  });
});
