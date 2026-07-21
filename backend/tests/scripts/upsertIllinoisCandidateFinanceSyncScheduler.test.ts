import { describe, expect, it } from "vitest";

import { parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertIllinoisCandidateFinanceSyncScheduler.js";

describe("upsertIllinoisCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs([
        "--max-candidates",
        "10",
        "--stale-after-days=3",
        "--lookback-days=7",
        "--lookahead-days=180",
        "--contributions-csv=/exports/il-contrib-a.csv",
        "--contributions-csv",
        "/exports/il-contrib-b.csv",
        "--expenditures-csv=/exports/il-exp.csv",
        "--contributions-url=https://example.test/contributions.csv",
        "--expenditures-url=https://example.test/expenditures.csv",
        "--normalized-artifact=/exports/illinois-normalized.json",
      ])
    ).toEqual({
      dryRun: false,
      force: false,
      maxCandidates: 10,
      staleAfterDays: 3,
      electionLookbackDays: 7,
      electionLookaheadDays: 180,
      contributionCsvPaths: ["/exports/il-contrib-a.csv", "/exports/il-contrib-b.csv"],
      expenditureCsvPaths: ["/exports/il-exp.csv"],
      contributionSourceUrl: "https://example.test/contributions.csv",
      expenditureSourceUrl: "https://example.test/expenditures.csv",
      normalizedArtifactPath: "/exports/illinois-normalized.json",
    });
  });

  it("rejects malformed numeric flags", () => {
    expect(() => parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs(["--lookahead-days=1.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
  });

  it("requires contribution artifacts when artifact source flags are provided", () => {
    expect(() => parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs(["--expenditures-csv=/exports/il-exp.csv"]))
      .toThrow("Provide --contributions-csv or --normalized-artifact when using Illinois SBE artifact flags");
    expect(() =>
      parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs([
        "--contributions-url=https://example.test/contributions.csv",
      ])
    ).toThrow("Provide --contributions-csv or --normalized-artifact when using Illinois SBE artifact flags");
    expect(
      parseUpsertIllinoisCandidateFinanceSyncSchedulerArgs([
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
