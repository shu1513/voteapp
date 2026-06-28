import { describe, expect, it } from "vitest";

import { parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertKentuckyCandidateFinanceSyncScheduler.js";

describe("upsertKentuckyCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--no-auto-link",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      autoLinkMissingLinks: false,
    });
  });

  it("defaults to auto-linking missing Kentucky KREF links", () => {
    expect(parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs([])).toMatchObject({
      autoLinkMissingLinks: true,
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

  it("rejects duplicate flag values", () => {
    expect(() =>
      parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(["--max-candidates=5", "--max-candidates", "10"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("rejects conflicting auto-link flags", () => {
    expect(() =>
      parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(["--auto-link", "--no-auto-link"])
    ).toThrow("Provide either --auto-link or --no-auto-link, not both");
  });

  it("rejects unknown flags instead of silently ignoring typos", () => {
    expect(() => parseUpsertKentuckyCandidateFinanceSyncSchedulerArgs(["--dryrun"])).toThrow(
      "Unknown Kentucky candidate finance scheduler upsert flag: --dryrun"
    );
  });
});
