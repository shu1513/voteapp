import { describe, expect, it } from "vitest";

import { parseSyncDueSanFranciscoCandidateFinanceScriptArgs } from "../../src/scripts/syncDueSanFranciscoCandidateFinance.js";

// Strict CLI contract shared with the other finance sync-due scripts: an
// operator typo must fail loudly, never silently run a REAL sync in place
// of the intended dry run or backfill preview.

describe("San Francisco finance sync-due CLI args", () => {
  const parse = parseSyncDueSanFranciscoCandidateFinanceScriptArgs;

  it("parses the full flag set", () => {
    const options = parse([
      "--dry-run",
      "--force",
      "--max-candidates",
      "5",
      "--stale-after-days",
      "3",
      "--lookback-days",
      "800",
      "--lookahead-days",
      "365",
      "--election-id",
      "8b1f5a2c-9d3e-4f10-8a2b-6c5d4e3f2a1b",
    ]);
    expect(options).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 800,
      electionLookaheadDays: 365,
      electionId: "8b1f5a2c-9d3e-4f10-8a2b-6c5d4e3f2a1b",
    });
  });

  it("defaults every option off", () => {
    expect(parse([])).toEqual({
      dryRun: false,
      force: false,
      maxCandidates: undefined,
      staleAfterDays: undefined,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
      electionId: undefined,
    });
  });

  it("rejects an unknown flag such as the --dryrun typo", () => {
    expect(() => parse(["--dryrun"])).toThrow(
      /Unknown San Francisco candidate finance flag: --dryrun/,
    );
  });

  it("rejects a bare positional after npm's own -- separator", () => {
    expect(() => parse(["dry-run"])).toThrow(
      /Unknown San Francisco candidate finance flag: dry-run/,
    );
  });

  it("rejects a non-positive-integer value", () => {
    expect(() => parse(["--max-candidates", "0"])).toThrow(
      /--max-candidates requires a positive integer, got: 0/,
    );
    expect(() => parse(["--lookback-days", "x"])).toThrow(
      /--lookback-days requires a positive integer/,
    );
  });

  it("rejects a missing value", () => {
    expect(() => parse(["--stale-after-days"])).toThrow(
      /--stale-after-days requires a positive integer/,
    );
  });

  it("rejects a malformed --election-id", () => {
    expect(() => parse(["--election-id", "2026-mayor"])).toThrow(
      /--election-id requires an election UUID, got: 2026-mayor/,
    );
    expect(() => parse(["--election-id"])).toThrow(
      /--election-id requires an election UUID/,
    );
  });
});
