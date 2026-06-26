import { describe, expect, it } from "vitest";

import { parseAlaskaCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerAlaskaCandidateFinanceSync.js";

describe("triggerAlaskaCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseAlaskaCandidateFinanceSyncTriggerArgs([
        "--write",
        "--force",
        "--auto-link",
        "--live",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--income-url=https://example.test/income.csv",
        "--timeout-ms=1000",
        "--retry-count=1",
      ])
    ).toMatchObject({
      dryRun: false,
      force: true,
      autoLinkMissingLinks: true,
      dataSourceMode: "live",
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      incomeUrl: "https://example.test/income.csv",
      timeoutMs: 1000,
      retryCount: 1,
    });
  });

  it("defaults manual triggers to dry-run", () => {
    expect(parseAlaskaCandidateFinanceSyncTriggerArgs(["--income-csv=/tmp/income.csv"])).toMatchObject({
      dryRun: true,
      autoLinkMissingLinks: false,
      incomeCsvPath: "/tmp/income.csv",
    });
  });

  it("rejects malformed integer and duplicate flags", () => {
    expect(() => parseAlaskaCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseAlaskaCandidateFinanceSyncTriggerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseAlaskaCandidateFinanceSyncTriggerArgs(["--income-csv=/tmp/a.csv", "--income-csv=/tmp/b.csv"])).toThrow(
      "Provide --income-csv at most once"
    );
    expect(() => parseAlaskaCandidateFinanceSyncTriggerArgs(["--live", "--income-csv=/tmp/income.csv"])).toThrow(
      "Do not provide --income-csv, --ie-expenditures-csv, or --ie-contributions-csv when using live mode"
    );
  });
});
