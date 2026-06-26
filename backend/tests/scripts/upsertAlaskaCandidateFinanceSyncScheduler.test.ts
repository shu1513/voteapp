import { describe, expect, it } from "vitest";

import { parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertAlaskaCandidateFinanceSyncScheduler.js";

describe("upsertAlaskaCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options with safe dry-run default", () => {
    expect(
      parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs([
        "--force",
        "--auto-link",
        "--max-candidates=5",
        "--stale-after-days=3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--income-csv=/tmp/income.csv",
        "--ie-expenditures-csv=/tmp/ie-exp.csv",
        "--timeout-ms=1000",
        "--retry-count=1",
        "--retry-delay-ms=0",
        "--request-spacing-ms=0",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      autoLinkMissingLinks: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      dataSourceMode: undefined,
      incomeCsvPath: "/tmp/income.csv",
      independentExpendituresCsvPath: "/tmp/ie-exp.csv",
      independentContributionsCsvPath: undefined,
      incomeUrl: undefined,
      independentExpendituresUrl: undefined,
      independentContributionsUrl: undefined,
      timeoutMs: 1000,
      retryCount: 1,
      retryDelayMs: 0,
      requestSpacingMs: 0,
    });
  });

  it("parses write and live mode explicitly", () => {
    expect(parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs(["--write", "--live"])).toMatchObject({
      dryRun: false,
      dataSourceMode: "live",
    });
  });

  it("rejects malformed and conflicting options", () => {
    expect(() => parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs(["--dry-run", "--write"])).toThrow(
      "Provide either --dry-run or --write, not both"
    );
    expect(() => parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs(["--live", "--csv"])).toThrow(
      "Provide either --live or --csv, not both"
    );
    expect(() => parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs(["--income-csv"])).toThrow(
      "Missing --income-csv value"
    );
    expect(() =>
      parseUpsertAlaskaCandidateFinanceSyncSchedulerArgs(["--live", "--income-csv=/tmp/income.csv"])
    ).toThrow("Do not provide --income-csv, --ie-expenditures-csv, or --ie-contributions-csv when using live mode");
  });
});
