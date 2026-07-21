import { describe, expect, it } from "vitest";

import { parseIllinoisCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerIllinoisCandidateFinanceSync.js";

describe("triggerIllinoisCandidateFinanceSync script", () => {
  it("parses manual scheduler trigger flags", () => {
    expect(
      parseIllinoisCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days=2",
        "--lookback-days=14",
        "--lookahead-days=365",
        "--contributions-csv=/exports/il-contrib-a.csv",
        "--contributions-csv",
        "/exports/il-contrib-b.csv",
        "--expenditures-csv=/exports/il-exp.csv",
        "--contributions-url=https://example.test/contributions.csv",
        "--expenditures-url=https://example.test/expenditures.csv",
        "--normalized-artifact=/exports/illinois-normalized.json",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 2,
      electionLookbackDays: 14,
      electionLookaheadDays: 365,
      contributionCsvPaths: ["/exports/il-contrib-a.csv", "/exports/il-contrib-b.csv"],
      expenditureCsvPaths: ["/exports/il-exp.csv"],
      contributionSourceUrl: "https://example.test/contributions.csv",
      expenditureSourceUrl: "https://example.test/expenditures.csv",
      normalizedArtifactPath: "/exports/illinois-normalized.json",
    });
  });

  it("rejects unknown flags", () => {
    expect(() => parseIllinoisCandidateFinanceSyncTriggerArgs(["--unknown"])).toThrow("Unknown option: --unknown");
  });

  it("requires contribution artifacts when artifact source flags are provided", () => {
    expect(() => parseIllinoisCandidateFinanceSyncTriggerArgs(["--expenditures-csv=/exports/il-exp.csv"]))
      .toThrow("Provide --contributions-csv or --normalized-artifact when using Illinois SBE artifact flags");
    expect(() =>
      parseIllinoisCandidateFinanceSyncTriggerArgs(["--contributions-url=https://example.test/contributions.csv"])
    ).toThrow("Provide --contributions-csv or --normalized-artifact when using Illinois SBE artifact flags");
    expect(
      parseIllinoisCandidateFinanceSyncTriggerArgs([
        "--normalized-artifact=/exports/illinois-normalized.json",
        "--expenditures-csv=/exports/il-exp.csv",
      ])
    ).toMatchObject({
      contributionCsvPaths: undefined,
      expenditureCsvPaths: ["/exports/il-exp.csv"],
      normalizedArtifactPath: "/exports/illinois-normalized.json",
    });
  });
});
