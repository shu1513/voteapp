import { describe, expect, it } from "vitest";

import { parseAutoLinkIdahoCandidateFinanceScriptArgs } from "../../src/scripts/autoLinkIdahoCandidateFinance.js";

describe("parseAutoLinkIdahoCandidateFinanceScriptArgs", () => {
  it("parses defaults and explicit flags", () => {
    expect(parseAutoLinkIdahoCandidateFinanceScriptArgs([])).toEqual({
      force: false,
      dryRun: false,
      maxCandidates: 25,
      electionLookbackDays: 98,
      electionLookaheadDays: 730,
    });
    expect(
      parseAutoLinkIdahoCandidateFinanceScriptArgs(["--dry-run", "--force", "--max-candidates=200", "--lookback-days", "10"])
    ).toEqual({ force: true, dryRun: true, maxCandidates: 200, electionLookbackDays: 10, electionLookaheadDays: 730 });
  });

  it("rejects unknown flags and bad values", () => {
    expect(() => parseAutoLinkIdahoCandidateFinanceScriptArgs(["--page-size=5"])).toThrow(
      "Unknown Idaho candidate finance auto-link flag"
    );
    expect(() => parseAutoLinkIdahoCandidateFinanceScriptArgs(["--max-candidates=0"])).toThrow("Invalid --max-candidates");
    expect(() => parseAutoLinkIdahoCandidateFinanceScriptArgs(["--max-candidates=1", "--max-candidates=2"])).toThrow(
      "at most once"
    );
  });
});
