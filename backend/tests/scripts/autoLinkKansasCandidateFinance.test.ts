import { describe, expect, it } from "vitest";

import { parseAutoLinkKansasCandidateFinanceScriptArgs } from "../../src/scripts/autoLinkKansasCandidateFinance.js";

describe("parseAutoLinkKansasCandidateFinanceScriptArgs", () => {
  it("applies defaults", () => {
    expect(parseAutoLinkKansasCandidateFinanceScriptArgs([])).toEqual({
      force: false,
      dryRun: false,
      maxCandidates: 25,
      electionLookbackDays: 98,
      electionLookaheadDays: 730,
    });
  });

  it("parses flags in both forms", () => {
    expect(
      parseAutoLinkKansasCandidateFinanceScriptArgs(["--force", "--dry-run", "--max-candidates=200", "--lookback-days", "10"])
    ).toEqual({ force: true, dryRun: true, maxCandidates: 200, electionLookbackDays: 10, electionLookaheadDays: 730 });
  });

  it("rejects unknown flags, positionals, and bad values", () => {
    expect(() => parseAutoLinkKansasCandidateFinanceScriptArgs(["--dryrun"])).toThrow("Unknown Kansas candidate finance auto-link flag");
    expect(() => parseAutoLinkKansasCandidateFinanceScriptArgs(["dry-run"])).toThrow("Unexpected positional argument");
    expect(() => parseAutoLinkKansasCandidateFinanceScriptArgs(["--dry-run=true"])).toThrow("Boolean flag does not accept a value");
    expect(() => parseAutoLinkKansasCandidateFinanceScriptArgs(["--max-candidates", "0"])).toThrow("Invalid --max-candidates value");
    expect(() => parseAutoLinkKansasCandidateFinanceScriptArgs(["--max-candidates"])).toThrow("Missing --max-candidates value");
  });
});
